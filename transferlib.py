import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import os.path
import os
import sys
import mimetypes
import math
from filechunkio import FileChunkIO
import multiprocessing
import time
import threading
try:
    import __builtin__ as builtins
except ImportError:
    import builtins
import socket
import tempfile


""" QUICK REFERENCE """

""" EXAMPLE """
"""
import transferlib

# some basic transfers
transferlib.s3_upload("./transferlib.py", "deaconjs.etc.scripts", "transferlib.py")
transferlib.s3_move("deaconjs.etc.scripts", "transferlib.py", "deaconjs.etc.packages", "transferlib/transferlib.py")
transferlib.s3_download("deaconjs.etc.packages", "transferlib.py", "./transferlib.backedup.py")

upload was taken from gist.github.com/fabiant7t/924094
download was taken from github.com/mumrah/s3-multipart/blob/master/s3-mp-download.py

"""


""" TRANSFER FUNCTIONS """

# copy the key from source to destination bucket under a new name 
# s3_copy(source_bucket_name, source_keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={})

# remove a key from a bucket
# s3_remove(key, bucket_name)

# move a key from one bucket to another
# s3_move(source_bucket_name, keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={})

# upload a file
# s3_upload(source_path, bucketname, keyname, headers={}, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), reduced_redundancy=False)

# download a file
# s3_download(bucketname, keyname, dest_path, headers={}, parallel_processes=multiprocessing.cpu_count())

# download a part of a file
# s3_download_partial(bucketname, keyname, dest_path, headers={}, parallel_processes=multiprocessing.cpu_count(), startbit, endbit)

# add the transfer record to the local log file
# s3_log_append(type, source, destination, size, start_time, total_time, passed_size_check, logfile="./transferlib.log")

""" MD5 FUNCTIONS """

# the following two just compare etags and will fail for large files.
# compare_md5_local_to_s3(local_file_name, original_bucket, original_loc):
# compare_md5_s3_to_s3(dest_bucket, dest_key, orig_bucket, orig_key):

# this one needs to be implemented to read manifest files for md5s, and will be custom per s3turbo implementation
# fetch_vendor_md5(vendor_bucket, batch, sample):

""" DOCUMENTATION """


"""
s3_copy(source_bucket_name, source_keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={})
    s3_copy requires a boto Key object, boto connect_s3 object, source and destination bucket names (strings), 
    and the new key name. The function can make a lazy guess at mimetype.

    Copy is performed in a parallelized manner. It uses MultiPartUpload.copy_part_from_key for the transfer, 
    and if this function throws an exception s3_copy retries up to a default of 10 times. If not all parts
    are transferred properly s3_copy cancels the upload and produces an error message. 


s3_remove(key, bucket_name) 
    s3_remove simply uses the boto bucket.delete_key function to remove a key
    requires a key object, a string for the bucket name, and a boto connect_s3 object


s3_move(source_bucket_name, keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={})
    s3_move uses the s3_copy function, followed by s3_delete. Before deleting the file size is compared between old 
    and new keys. If the file size is different s3_move produces an error message and exits. See s3_copy for a
    decription of arguments.


s3_upload(source_path, bucketname, keyname, headers={}, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), reduced_redundancy=False)
    s3_upload requires a destination bucket name, the path to the local file, a key name to store it under, and a 
    boto connect_s3 object. headers can be included and s3_upload can make a guess as to mimetype.

    For files over 50 Mb, multi-part upload is performed in a parallelized manner, with a default of 8 threads. It uses 
    MultiPartUpload.copy_part_from_key for the transfer, and if this function throws an exception s3_copy retries 
    up to a default of 10 times. If not all parts are transferred properly s3_copy cancels the upload and produces 
    an error message. 

s3_download(bucketname, keyname, dest_path, headers={}, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count())
    s3_upload requires source bucket and key names, the destination path, and a boto connect_s3 object. For files over
    50 Mb, multi-part download is performed in a parallel manner, with a default of multiprocessing.cpu_count() threads.

s3_log_append(source, destination, size, start_time, total_time, logfile="./transferlib.log")

"""
 



""" FUNCTIONS """

def _is_in_multiprocessing_context():
    """Check if we're currently running in a multiprocessing worker"""
    try:
        import multiprocessing
        current_process = multiprocessing.current_process()
        return current_process.name != 'MainProcess'
    except:
        return False

def get_s3_session(profile_name=None):
    """Create a boto3 session with proper SSO support"""
    try:
        if profile_name:
            session = boto3.Session(profile_name=profile_name)
        else:
            session = boto3.Session()
        
        # Test the credentials by making a simple call
        s3_client = session.client('s3')
        s3_client.list_buckets()
        return session
    except NoCredentialsError:
        print("Error: No AWS credentials found. Please run 'aws configure' or 'aws sso login' if using SSO")
        sys.exit(1)
    except ClientError as e:
        if 'ExpiredToken' in str(e) or 'InvalidToken' in str(e):
            print("Error: AWS credentials have expired. Please run 'aws sso login' if using SSO")
            sys.exit(1)
        else:
            print(f"Error accessing AWS: {e}")
            sys.exit(1)

def get_s3_client(profile_name=None, use_accelerate=False, timeout_config=None):
    """Get S3 client with proper authentication and optional transfer acceleration"""
    session = get_s3_session(profile_name)
    
    if use_accelerate or timeout_config:
        # Use S3 Transfer Acceleration for faster transfers (if bucket supports it)
        from botocore.config import Config
        
        # Default timeout configuration for reliability
        if timeout_config is None:
            timeout_config = {
                'connect_timeout': 60,
                'read_timeout': 120  # 2 minutes for large chunks
            }
        
        config_params = {
            'retries': {'max_attempts': 5, 'mode': 'adaptive'},
            'max_pool_connections': 50,
            **timeout_config
        }
        
        if use_accelerate:
            config_params.update({
                'region_name': 'us-east-1',  # Transfer acceleration uses global endpoints
                's3': {'use_accelerate_endpoint': True}
            })
            
        config = Config(**config_params)
    else:
        config = None
    
    return session.client('s3', config=config)

def get_s3_resource(profile_name=None):
    """Get S3 resource with proper authentication"""  
    session = get_s3_session(profile_name)
    return session.resource('s3')

def s3_copy(source_bucket_name, source_keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={}, quiet=False, reduced_redundancy=False):
    s3_client = get_s3_client()
    s3_resource = get_s3_resource()
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    t1 = time.time()
    
    try:
        # Get source bucket and check if key exists
        source_bucket = s3_resource.Bucket(source_bucket_name)
        source_obj = source_bucket.Object(source_keyname)
        source_obj.load()  # This will raise an exception if object doesn't exist
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'NoSuchBucket':
            print("Fatal Error: source bucket %s not found: exception %s"%(source_bucket_name, exc))
        elif exc.response['Error']['Code'] == 'NoSuchKey':
            print("Fatal Error in s3_copy: source key %s not found in bucket %s"%(source_keyname, source_bucket_name))
        else:
            print("Fatal Error: %s"%(exc))
        sys.exit()
    
    try:
        # Get destination bucket
        dest_bucket = s3_resource.Bucket(dest_bucket_name)
        dest_bucket.load()  # This will raise an exception if bucket doesn't exist
    except ClientError as exc:
        print("Fatal Error: destination bucket %s not found: exception %s"%(dest_bucket_name, exc))
        sys.exit()
    if guess_mimetype:
        mtype = mimetypes.guess_type(source_obj.key)[0] or 'application/octet-stream'
        headers.update({'Content-Type':mtype})
    source_size = source_obj.content_length
    bytes_per_chunk = 50 * 1024 * 1024
    chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))
    while chunk_amount > 200:
        bytes_per_chunk += 5242880
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    size_mb = source_size / 1024 / 1024
    if not quiet:
        print("copying %s %s b (%s) in %s chunks"%(source_keyname, source_size, size_mb, chunk_amount))
    if source_size < bytes_per_chunk:
        # Simple copy for small files
        copy_source = {
            'Bucket': source_bucket_name,
            'Key': source_keyname
        }
        extra_args = {}
        if reduced_redundancy:
            extra_args['StorageClass'] = 'REDUCED_REDUNDANCY'
        if headers:
            extra_args['Metadata'] = headers
        s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket_name, Key=newkeyname, **extra_args)
    else:
        # Multipart copy for large files
        create_args = {'Bucket': dest_bucket_name, 'Key': newkeyname}
        if reduced_redundancy:
            create_args['StorageClass'] = 'REDUCED_REDUNDANCY'
        if headers:
            create_args['Metadata'] = headers
        
        mp = s3_client.create_multipart_upload(**create_args)
        upload_id = mp['UploadId']
        
        builtins.global_download_total = chunk_amount
        builtins.global_download_progress = 0
        builtins.global_file_size = source_size
        builtins.global_file_size_mb = source_size / 1024 / 1024
        builtins.global_download_start_time = time.time()
        
        # Use sequential processing if we're already in a multiprocessing context
        if _is_in_multiprocessing_context():
            for i in range(chunk_amount):
                offset = i * bytes_per_chunk
                remaining_bytes = source_size - offset
                bytes_to_copy = min([bytes_per_chunk, remaining_bytes])
                part_num = i + 1
                _copy_part(dest_bucket_name, newkeyname, upload_id, part_num, source_bucket_name, source_keyname, int(offset), int(bytes_to_copy), parallel_processes, quiet)
        else:
            pool = multiprocessing.Pool(processes=parallel_processes)
            for i in range(chunk_amount):
                offset = i * bytes_per_chunk
                remaining_bytes = source_size - offset
                bytes_to_copy = min([bytes_per_chunk, remaining_bytes])
                part_num = i + 1
                pool.apply_async(_copy_part, [dest_bucket_name, newkeyname, upload_id, part_num, source_bucket_name, source_keyname, int(offset), int(bytes_to_copy), parallel_processes, quiet])
            pool.close()
            pool.join()
        
        # List completed parts and finish multipart upload
        parts_response = s3_client.list_parts(Bucket=dest_bucket_name, Key=newkeyname, UploadId=upload_id)
        completed_parts = parts_response.get('Parts', [])
        
        if len(completed_parts) == chunk_amount:
            # Complete the multipart upload
            parts_list = [{'ETag': part['ETag'], 'PartNumber': part['PartNumber']} for part in completed_parts]
            s3_client.complete_multipart_upload(
                Bucket=dest_bucket_name,
                Key=newkeyname,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts_list}
            )
        else:
            print("cancelling copy for %s - %d chunks uploaded, %d needed\n"%(source_keyname, len(completed_parts), chunk_amount))
            s3_client.abort_multipart_upload(Bucket=dest_bucket_name, Key=newkeyname, UploadId=upload_id)

    if not quiet:
        print()
    t2 = time.time()-t1
    source = "S3:%s:%s"%(source_bucket_name, source_keyname)
    destination = "S3:%s:%s"%(dest_bucket_name, newkeyname)
    
    # Get final object size
    try:
        dest_obj = s3_resource.Object(dest_bucket_name, newkeyname)
        size = dest_obj.content_length
    except ClientError:
        size = 0
    total_time = t2
    if size == source_size:
        passed_size_check = True
    else:
        passed_size_check = False
    s3_log_append("s3_copy", source, destination, size, start_time, total_time, passed_size_check, logfile="./itmi_s3lib.log")
    if not quiet:
        print("Finished copying %0.2fM in %0.2fs (%0.2fmbps)"%(size/1024/1024, t2, 8*size_mb/t2))

def _copy_part(dest_bucket_name, dest_key_name, upload_id, part_num, source_bucket_name, source_key_name, offset, bytes_to_copy, threads, quiet, amount_of_retries=10):
    def _copy(retries_left=amount_of_retries):
        try:
            s3_client = get_s3_client()
            copy_source = {
                'Bucket': source_bucket_name,
                'Key': source_key_name,
                'Range': 'bytes=%d-%d' % (offset, offset + bytes_to_copy - 1)
            }
            s3_client.upload_part_copy(
                CopySource=copy_source,
                Bucket=dest_bucket_name,
                Key=dest_key_name,
                PartNumber=part_num,
                UploadId=upload_id
            )
        except Exception as exc:
            print("failed partial upload, exception %s\nRetries left %s"%(exc, retries_left-1))
            if retries_left:
                _copy(retries_left=retries_left-1)
            else:
                raise exc
    _copy()
    
    builtins.global_download_progress += 1
    g = builtins.global_download_progress
    t = builtins.global_download_total
    if not quiet:
        progress_percent = int(100*g/(0.0+t)) if t > 0 else 0
        # Calculate downloaded data with real-time info including speed and ETA
        if hasattr(builtins, 'global_file_size') and hasattr(builtins, 'global_file_size_mb'):
            downloaded_mb = (g / t) * builtins.global_file_size_mb if t > 0 else 0
            total_mb = builtins.global_file_size_mb
            
            # Calculate download speed and ETA
            if hasattr(builtins, 'global_download_start_time') and downloaded_mb > 0:
                elapsed_time = time.time() - builtins.global_download_start_time
                if elapsed_time > 0:
                    speed_mbps = downloaded_mb / elapsed_time
                    remaining_mb = total_mb - downloaded_mb
                    eta_seconds = remaining_mb / speed_mbps if speed_mbps > 0 else 0
                    eta_str = f" ETA: {int(eta_seconds//60)}m{int(eta_seconds%60)}s" if eta_seconds > 0 else ""
                    speed_str = f" {speed_mbps:.1f} MB/s"
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB{speed_str}{eta_str}")
                else:
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
            else:
                sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
        else:
            sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks)")
        sys.stdout.flush()


def s3_remove(bucket_name, key_name):
    s3_client = get_s3_client()
    try:
        # Check if object exists before deleting
        s3_client.head_object(Bucket=bucket_name, Key=key_name)
        s3_client.delete_object(Bucket=bucket_name, Key=key_name)
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'NoSuchBucket':
            print("Fatal Error: source bucket %s not found, exception %s"%(bucket_name, exc))
            sys.exit()
        elif exc.response['Error']['Code'] == 'NoSuchKey':
            print("Fatal Error in remove: source key %s not found in bucket %s"%(key_name, bucket_name))
            sys.exit()
        else:
            print("Fatal Error: %s"%(exc))
            sys.exit()


def s3_move(source_bucket_name, keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={}, quiet=False):
    s3_resource = get_s3_resource()
    try:
        # Check if source object exists
        source_obj = s3_resource.Object(source_bucket_name, keyname)
        source_obj.load()
        old_size = source_obj.content_length
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'NoSuchBucket':
            print("Fatal Error: source bucket %s not found, exception %s"%(source_bucket_name, exc))
        elif exc.response['Error']['Code'] == 'NoSuchKey':
            print("Fatal Error in move: key %s not found in bucket %s"%(keyname, source_bucket_name))
        else:
            print("Fatal Error: %s"%(exc))
        sys.exit()
    
    # Copy the object
    s3_copy(source_bucket_name, keyname, dest_bucket_name, newkeyname, guess_mimetype, parallel_processes, headers, quiet)
    
    # Verify the copy and delete original
    try:
        dest_obj = s3_resource.Object(dest_bucket_name, newkeyname)
        dest_obj.load()
        new_size = dest_obj.content_length
        
        if new_size == old_size:
            s3_remove(source_bucket_name, keyname)
        else:
            print("Fatal S3 copy error")
            print("source key %s from %s size %s is different from destination key %s from %s size %s"%(keyname, source_bucket_name, old_size, newkeyname, dest_bucket_name, new_size))
            sys.exit()
    except ClientError as exc:
        print("Fatal Error verifying moved object: %s"%(exc))
        sys.exit()

def _upload_part(bucketname, multipart_id, part_num, source_path, offset, bytes, conn, threads, quiet, amount_of_retries=10):
    def _upload(retries_left=amount_of_retries):
        try:
            bucket = conn.get_bucket(bucketname)
            for mp in bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
                        mp.upload_part_from_file(fp=fp, part_num=part_num)
        except Exception as exc:
            print("failed multi-part upload attempt, exception %s"%(exc))
            sys.exit()
            if retries_left:
                _upload(retries_left=retries_left-1)
            else:
                raise exc
    _upload()

    builtins.global_download_progress += 1
    g = builtins.global_download_progress
    t = builtins.global_download_total
    if not quiet:
        progress_percent = int(100*g/(0.0+t)) if t > 0 else 0
        # Calculate downloaded data with real-time info including speed and ETA
        if hasattr(builtins, 'global_file_size') and hasattr(builtins, 'global_file_size_mb'):
            downloaded_mb = (g / t) * builtins.global_file_size_mb if t > 0 else 0
            total_mb = builtins.global_file_size_mb
            
            # Calculate download speed and ETA
            if hasattr(builtins, 'global_download_start_time') and downloaded_mb > 0:
                elapsed_time = time.time() - builtins.global_download_start_time
                if elapsed_time > 0:
                    speed_mbps = downloaded_mb / elapsed_time
                    remaining_mb = total_mb - downloaded_mb
                    eta_seconds = remaining_mb / speed_mbps if speed_mbps > 0 else 0
                    eta_str = f" ETA: {int(eta_seconds//60)}m{int(eta_seconds%60)}s" if eta_seconds > 0 else ""
                    speed_str = f" {speed_mbps:.1f} MB/s"
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB{speed_str}{eta_str}")
                else:
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
            else:
                sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
        else:
            sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks)")
        sys.stdout.flush()


def s3_upload(source_path, bucketname, keyname, headers={}, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), quiet=False, reduced_redundancy=False):
    s3_client = get_s3_client()
    s3_resource = get_s3_resource()
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    t1 = time.time()
    if not os.path.isfile(source_path):
        print("Fatal Error: source path %s does not exist or is not a file"%(source_path))
        sys.exit()
    source_size = os.stat(source_path).st_size
    bytes_per_chunk = 50 * 1024 * 1024
    
    try:
        bucket = s3_resource.Bucket(bucketname)
        bucket.load()
    except ClientError as exc:
        print("Fatal Error: destination bucket %s does not exist - exception %s"%(bucketname, exc))
        sys.exit()

    if source_size <= bytes_per_chunk:
        # Simple upload for small files
        extra_args = {}
        if reduced_redundancy:
            extra_args['StorageClass'] = 'REDUCED_REDUNDANCY'
        if headers:
            extra_args['Metadata'] = headers
        s3_client.upload_file(source_path, bucketname, keyname, ExtraArgs=extra_args)
    else:
        # Multipart upload for large files
        if guess_mimetype:
            mtype = mimetypes.guess_type(keyname)[0] or 'application/octet-stream'
            headers.update({'Content-Type':mtype})
        
        create_args = {'Bucket': bucketname, 'Key': keyname}
        if reduced_redundancy:
            create_args['StorageClass'] = 'REDUCED_REDUNDANCY'
        if headers:
            create_args['Metadata'] = headers
        
        mp = s3_client.create_multipart_upload(**create_args)
        upload_id = mp['UploadId']
 
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))

        while chunk_amount > 200:
            bytes_per_chunk += 5242880
            chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))
        builtins.global_download_total = chunk_amount
        builtins.global_download_progress = 0
        builtins.global_file_size = source_size
        builtins.global_file_size_mb = source_size / 1024 / 1024
        builtins.global_download_start_time = time.time()

        size_mb = source_size / 1024 / 1024
        if not quiet:
            print("uploading %s (%s) in %s chunks"%(source_size, size_mb, chunk_amount))
        
        # Use sequential processing if we're already in a multiprocessing context
        if _is_in_multiprocessing_context():
            for i in range(chunk_amount):
                offset = i*bytes_per_chunk
                remaining_bytes = source_size - offset
                bytes_to_upload = min([bytes_per_chunk, remaining_bytes])
                part_num = i + 1
                _upload_part(bucketname, keyname, upload_id, part_num, source_path, offset, bytes_to_upload, parallel_processes, quiet)
        else:
            pool = multiprocessing.Pool(processes=parallel_processes)
            for i in range(chunk_amount):
                offset = i*bytes_per_chunk
                remaining_bytes = source_size - offset
                bytes_to_upload = min([bytes_per_chunk, remaining_bytes])
                part_num = i + 1
                pool.apply_async(_upload_part, [bucketname, keyname, upload_id, part_num, source_path, offset, bytes_to_upload, parallel_processes, quiet])
            pool.close()
            pool.join()
        
        # List completed parts and finish multipart upload
        parts_response = s3_client.list_parts(Bucket=bucketname, Key=keyname, UploadId=upload_id)
        completed_parts = parts_response.get('Parts', [])
        
        if len(completed_parts) == chunk_amount:
            # Complete the multipart upload
            parts_list = [{'ETag': part['ETag'], 'PartNumber': part['PartNumber']} for part in completed_parts]
            s3_client.complete_multipart_upload(
                Bucket=bucketname,
                Key=keyname,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts_list}
            )
        else:
            print("canceling upload for %s - %d chunks uploaded, %d needed\n"%(source_path, len(completed_parts), chunk_amount))
            s3_client.abort_multipart_upload(Bucket=bucketname, Key=keyname, UploadId=upload_id)
    
    if not quiet:
        print()
    t2 = time.time()-t1
    source = "local:%s:%s"%(socket.gethostname(), source_path)
    destination = "S3:%s:%s"%(bucketname, keyname)
    
    # Get final object size
    try:
        obj = s3_resource.Object(bucketname, keyname)
        size = obj.content_length
    except ClientError:
        size = 0
    
    total_time = t2
    if source_size == size:
        passed_size_check = True
    else:
        passed_size_check = False
    s3_log_append("s3_upload", source, destination, size, start_time, total_time, passed_size_check, logfile="./itmi_s3lib.log")
    if not quiet:
        print("Finished uploading %0.2fM in %0.2fs (%0.2fmbps)"%(size/1024/1024, t2, 8*size/t2))


def _upload_part(bucketname, keyname, upload_id, part_num, source_path, offset, bytes_to_upload, threads, quiet, amount_of_retries=10):
    def _upload(retries_left=amount_of_retries):
        try:
            s3_client = get_s3_client()
            with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes_to_upload) as fp:
                s3_client.upload_part(
                    Bucket=bucketname,
                    Key=keyname,
                    PartNumber=part_num,
                    UploadId=upload_id,
                    Body=fp
                )
        except Exception as exc:
            print("failed multi-part upload attempt, exception %s"%(exc))
            if retries_left:
                _upload(retries_left=retries_left-1)
            else:
                raise exc
    _upload()
    
    builtins.global_download_progress += 1
    g = builtins.global_download_progress
    t = builtins.global_download_total
    if not quiet:
        progress_percent = int(100*g/(0.0+t)) if t > 0 else 0
        # Calculate downloaded data with real-time info including speed and ETA
        if hasattr(builtins, 'global_file_size') and hasattr(builtins, 'global_file_size_mb'):
            downloaded_mb = (g / t) * builtins.global_file_size_mb if t > 0 else 0
            total_mb = builtins.global_file_size_mb
            
            # Calculate download speed and ETA
            if hasattr(builtins, 'global_download_start_time') and downloaded_mb > 0:
                elapsed_time = time.time() - builtins.global_download_start_time
                if elapsed_time > 0:
                    speed_mbps = downloaded_mb / elapsed_time
                    remaining_mb = total_mb - downloaded_mb
                    eta_seconds = remaining_mb / speed_mbps if speed_mbps > 0 else 0
                    eta_str = f" ETA: {int(eta_seconds//60)}m{int(eta_seconds%60)}s" if eta_seconds > 0 else ""
                    speed_str = f" {speed_mbps:.1f} MB/s"
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB{speed_str}{eta_str}")
                else:
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
            else:
                sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
        else:
            sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks)")
        sys.stdout.flush()


def _download_part(args):
    bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet = args
    
    if not quiet:
        chunk_size_mb = (max_byte - min_byte + 1) / 1024 / 1024
        print(f"[DEBUG] Starting download part: {min_byte}-{max_byte} ({chunk_size_mb:.1f} MB)")
    
    try:
        # Use timeout configuration for this specific request
        timeout_config = {
            'connect_timeout': 30,
            'read_timeout': 90  # 1.5 minutes per chunk
        }
        s3_client = get_s3_client(timeout_config=timeout_config)
        
        # Download the specific byte range
        if not quiet:
            print(f"[DEBUG] Requesting bytes {min_byte}-{max_byte} from s3://{bucketname}/{keyname}")
            
        response = s3_client.get_object(
            Bucket=bucketname,
            Key=keyname,
            Range='bytes=%d-%d' % (min_byte, max_byte)
        )
        
        if not quiet:
            print(f"[DEBUG] Got response, reading data...")
        
        # Read and write the data with streaming to handle large chunks
        data = response['Body'].read()
        data_size = len(data)
        
        if not quiet:
            print(f"[DEBUG] Read {data_size} bytes, writing to file at position {min_byte}")
        
        # Write to the specific position in the file
        fd = os.open(fname, os.O_WRONLY)
        os.lseek(fd, min_byte, os.SEEK_SET)
        os.write(fd, data)
        os.close(fd)
        
        if not quiet:
            print(f"[DEBUG] Successfully wrote {data_size} bytes to {fname}")
        
    except Exception as err:
        error_str = str(err)
        if not quiet:
            print(f"[ERROR] Download part failed: {err}")
            print(f"[ERROR] Range: {min_byte}-{max_byte}, File: {fname}")
        
        # Check if it's a timeout error that might benefit from smaller chunks
        is_timeout = any(timeout_keyword in error_str.lower() for timeout_keyword in 
                        ['timeout', 'timed out', 'connection reset', 'connection broken'])
        
        if current_tries >= max_tries:
            if is_timeout and not quiet:
                print(f"[SUGGESTION] Consider using --debug mode for smaller, more reliable chunks")
            print("Error downloading, %s"%(err))
            raise err
        else:
            retry_delay = 3 + (current_tries * 2)  # Exponential backoff
            if not quiet:
                if is_timeout:
                    print(f"[RETRY] Network timeout detected - retrying with {retry_delay}s delay (attempt {current_tries + 1}/{max_tries})")
                else:
                    print(f"[RETRY] Retrying download part (attempt {current_tries + 1}/{max_tries})")
            time.sleep(retry_delay)
            current_tries += 1
            new_args = (bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet)
            return _download_part(new_args)

    builtins.global_download_progress += 1
    g = builtins.global_download_progress
    t = builtins.global_download_total
    if not quiet:
        progress_percent = int(100*g/(0.0+t)) if t > 0 else 0
        # Calculate downloaded data with real-time info including speed and ETA
        if hasattr(builtins, 'global_file_size') and hasattr(builtins, 'global_file_size_mb'):
            downloaded_mb = (g / t) * builtins.global_file_size_mb if t > 0 else 0
            total_mb = builtins.global_file_size_mb
            
            # Calculate download speed and ETA
            if hasattr(builtins, 'global_download_start_time') and downloaded_mb > 0:
                elapsed_time = time.time() - builtins.global_download_start_time
                if elapsed_time > 0:
                    speed_mbps = downloaded_mb / elapsed_time
                    remaining_mb = total_mb - downloaded_mb
                    eta_seconds = remaining_mb / speed_mbps if speed_mbps > 0 else 0
                    eta_str = f" ETA: {int(eta_seconds//60)}m{int(eta_seconds%60)}s" if eta_seconds > 0 else ""
                    speed_str = f" {speed_mbps:.1f} MB/s"
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB{speed_str}{eta_str}")
                else:
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
            else:
                sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
        else:
            sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks)")
        sys.stdout.flush()

def _download_part_with_double_check(args):
    conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet = args
    resp1 = conn.make_request("GET", bucket=bucketname, key=keyname, headers={'Range':"bytes=%d-%d"%(min_byte, max_byte)})
    resp2 = conn.make_request("GET", bucket=bucketname, key=keyname, headers={'Range':"bytes=%d-%d"%(min_byte, max_byte)})
    buc = conn.get_bucket('itmi.run.etc')
    #k = boto.s3.key.Key(buc)
    #k.key = 'rm_me_asdf'
    #k.set_contents_from_stream(resp.fp)
    fd = os.open(fname, os.O_WRONLY)
    os.lseek(fd, min_byte, os.SEEK_SET)
    chunk_size = min((max_byte-min_byte), split*1024*1024)
    s = 0
    try:
        while True:
            data1 = resp1.read(chunk_size)
            data2 = resp2.read(chunk_size)
            if data1 == "":
                break
            if data1 == data2:
                os.write(fd, data1)
                s += len(data1)
            else:
                time.sleep(3)
                current_tries += 1
                _download_part_with_double_check((conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet))
        os.close(fd)
        s = s / chunk_size
    except Exception as err:
        if (current_tries > max_tries):
            print("Error downloading, %s"%(err))
            sys.exit()
        else:
            time.sleep(3)
            current_tries += 1
            _download_part_with_double_check((conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet))

    builtins.global_download_progress += 1
    g = builtins.global_download_progress
    t = builtins.global_download_total
    if not quiet:
        progress_percent = int(100*g/(0.0+t)) if t > 0 else 0
        # Calculate downloaded data with real-time info including speed and ETA
        if hasattr(builtins, 'global_file_size') and hasattr(builtins, 'global_file_size_mb'):
            downloaded_mb = (g / t) * builtins.global_file_size_mb if t > 0 else 0
            total_mb = builtins.global_file_size_mb
            
            # Calculate download speed and ETA
            if hasattr(builtins, 'global_download_start_time') and downloaded_mb > 0:
                elapsed_time = time.time() - builtins.global_download_start_time
                if elapsed_time > 0:
                    speed_mbps = downloaded_mb / elapsed_time
                    remaining_mb = total_mb - downloaded_mb
                    eta_seconds = remaining_mb / speed_mbps if speed_mbps > 0 else 0
                    eta_str = f" ETA: {int(eta_seconds//60)}m{int(eta_seconds%60)}s" if eta_seconds > 0 else ""
                    speed_str = f" {speed_mbps:.1f} MB/s"
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB{speed_str}{eta_str}")
                else:
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
            else:
                sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
        else:
            sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks)")
        sys.stdout.flush()

def _download_part_with_size_check(args):
    conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet = args
    resp = conn.make_request("GET", bucket=bucketname, key=keyname, headers={'Range':"bytes=%d-%d"%(min_byte, max_byte)})
    fd = os.open(fname, os.O_WRONLY)
    os.lseek(fd, min_byte, os.SEEK_SET)
    chunk_size = min((max_byte-min_byte), split*1024*1024)
    s=0
    size_difference = -37
    second_message_size = 38
    try:
        while True:
            data = resp.read(chunk_size)
            if data == "":
                break
            sz = sys.getsizeof(data)
            #print("testing packet  %s %s %s %s attempt %s, %s, %s"%(chunk_size, sz, chunk_size-sz, size_difference, current_tries, min_byte, max_byte-min_byte))
            if chunk_size - sz != size_difference and sz != second_message_size:
                print("failed packet. retrying %s %s %s %s attempt %s, %s, %s"%(chunk_size, sz, chunk_size-sz, size_difference, current_tries, min_byte, max_byte-min_byte))
                time.sleep(1)
                current_tries += 1
                _download_part_with_size_check((conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet))
            os.write(fd, data)
            s += len(data)
        os.close(fd)
        s = s / chunk_size
    except Exception as err:
        if (current_tries > max_tries):
            print("Error downloading, %s"%(err))
            sys.exit()
        else:
            time.sleep(3)
            current_tries += 1
            _download_part_with_size_check((conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet))

    builtins.global_download_progress += 1
    g = builtins.global_download_progress
    t = builtins.global_download_total
    if not quiet:
        progress_percent = int(100*g/(0.0+t)) if t > 0 else 0
        # Calculate downloaded data with real-time info including speed and ETA
        if hasattr(builtins, 'global_file_size') and hasattr(builtins, 'global_file_size_mb'):
            downloaded_mb = (g / t) * builtins.global_file_size_mb if t > 0 else 0
            total_mb = builtins.global_file_size_mb
            
            # Calculate download speed and ETA
            if hasattr(builtins, 'global_download_start_time') and downloaded_mb > 0:
                elapsed_time = time.time() - builtins.global_download_start_time
                if elapsed_time > 0:
                    speed_mbps = downloaded_mb / elapsed_time
                    remaining_mb = total_mb - downloaded_mb
                    eta_seconds = remaining_mb / speed_mbps if speed_mbps > 0 else 0
                    eta_str = f" ETA: {int(eta_seconds//60)}m{int(eta_seconds%60)}s" if eta_seconds > 0 else ""
                    speed_str = f" {speed_mbps:.1f} MB/s"
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB{speed_str}{eta_str}")
                else:
                    sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
            else:
                sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks) {downloaded_mb:.1f}/{total_mb:.1f} MB")
        else:
            sys.stdout.write(f"\rProgress: {progress_percent}% ({g}/{t} chunks)")
        sys.stdout.flush()


def _gen_byte_ranges(size, num_parts):
    part_size = int(math.ceil(1. * size / num_parts))
    for i in range(num_parts):
    #for i in range(50, 60):
        yield(part_size*i, min(part_size*(i+1)-1, size-1), i)


# taken from github.com/mumrah/s3-multipart/blob/master/s3-mp-download.py
def s3_download(bucketname, keyname, dest_path, quiet=False, parallel_processes=None, headers={}, guess_mimetype=True, debug_mode=False, conservative_mode=False):
    # Configure timeouts for reliability
    timeout_config = {
        'connect_timeout': 30,
        'read_timeout': 60 if debug_mode else 120  # Shorter timeout in debug mode
    }
    
    s3_client = get_s3_client(timeout_config=timeout_config)
    s3_resource = get_s3_resource()
    
    # Adaptive chunk size based on connection quality
    # Start with smaller chunks in debug/conservative mode
    if conservative_mode:
        split = 10  # Very small chunks for maximum stability
        if not quiet:
            print("[CONSERVATIVE] Using 10MB chunks for maximum network stability")
    elif debug_mode:
        split = 25  # Small chunks for debugging
    else:
        split = 50  # Normal chunks
    
    max_tries = 5  # More retries for timeout issues
    
    # Auto-tune concurrency: For I/O-bound operations like downloads, 
    # we can use more workers than CPU count
    if parallel_processes is None:
        if debug_mode:
            parallel_processes = multiprocessing.cpu_count()  # Conservative in debug mode
        else:
            parallel_processes = min(multiprocessing.cpu_count() * 4, 32)  # 4x CPU count, max 32
        
    # Configure boto3 for optimal transfers (disabled in debug mode)
    if not debug_mode:
        try:
            from boto3.s3.transfer import TransferConfig
            config = TransferConfig(
                multipart_threshold=1024 * 25,  # 25MB (start multipart sooner)
                max_concurrency=parallel_processes,
                multipart_chunksize=1024 * 1024 * split,  # Our chunk size
                num_download_attempts=max_tries,
                max_io_queue=1000  # Increase I/O queue for better performance
            )
            use_optimized_transfer = True
        except ImportError:
            use_optimized_transfer = False
    else:
        use_optimized_transfer = False
        if not quiet:
            print("[DEBUG] Optimizations disabled - using basic download method")
    
    try:
        # Check if bucket and object exist
        bucket = s3_resource.Bucket(bucketname)
        obj = bucket.Object(keyname)
        obj.load()
        source_size = obj.content_length
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'NoSuchBucket':
            print("Fatal Error: source bucket %s not found, exception %s"%(bucketname, exc))
        elif exc.response['Error']['Code'] == 'NoSuchKey':
            print("Fatal Error in download: key %s not found in bucket %s"%(keyname, bucketname))
        else:
            print("Fatal Error: %s"%(exc))
        sys.exit()
    
    bytes_per_chunk = split * 1024 * 1024
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    
    if '/' in dest_path and not os.path.isdir(os.path.dirname(dest_path)):
        print("Fatal Error: destination path does not exist - args %s %s %s"%(bucketname, keyname, dest_path))
        sys.exit()
    if os.path.isdir(dest_path):
        print("Fatal Error: please supply file name in directory %s to store the file"%(dest_path))
        sys.exit()
    size_mb = source_size / 1024 / 1024
    t1 = time.time()
    num_parts = int((size_mb+(-size_mb%split))//split)
    while num_parts > 200:
        split += 5
        num_parts = int((size_mb+(-size_mb%split))//split)
    builtins.global_download_total = num_parts
    builtins.global_download_progress = 0
    builtins.global_file_size = source_size
    builtins.global_file_size_mb = size_mb
    builtins.global_download_start_time = time.time()
    if not quiet:
        print("downloading %s bytes (%.1f MB) in %s chunks using %d workers"%(source_size, size_mb, num_parts, parallel_processes))
    
    # Try optimized boto3 transfer first (fastest method)
    if use_optimized_transfer and not _is_in_multiprocessing_context():
        try:
            if not quiet:
                print("Using optimized boto3 transfer...")
            
            # Add timeout and better error handling
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError("Download timeout after 30 seconds")
            
            # Set timeout for hanging downloads
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(30)  # 30 second timeout
            
            try:
                s3_client.download_file(bucketname, keyname, dest_path, Config=config)
                signal.alarm(0)  # Cancel timeout
                if not quiet:
                    print("Optimized download completed successfully!")
                return
            except TimeoutError:
                signal.alarm(0)
                if not quiet:
                    print("Optimized transfer timed out, falling back to manual multipart")
            except Exception as inner_exc:
                signal.alarm(0)
                if not quiet:
                    print("Optimized transfer failed: %s, falling back to manual multipart"%(inner_exc))
                    
        except Exception as exc:
            if not quiet:
                print("Error setting up optimized transfer: %s, falling back to manual multipart"%(exc))
    
    # Fallback to manual multipart download
    if source_size <= bytes_per_chunk:
        try:
            s3_client.download_file(bucketname, keyname, dest_path)
        except Exception as exc:
            print("problem downloading key %s - exception %s"%(keyname, exc))
    else:
        # Create empty file for multipart download
        fd = os.open(dest_path, os.O_CREAT)
        os.close(fd)

        # Use optimized concurrent downloads
        # Always use parallel processing for downloads - it's safe and much faster
        if not quiet:
            print(f"Starting parallel download with {parallel_processes} workers...")
        
        # Check if we should use multiprocessing vs threading
        use_multiprocessing = debug_mode or conservative_mode
        
        if use_multiprocessing:
            # Use multiprocessing for debug/conservative mode for better reliability
            if not quiet:
                mode_name = "Debug" if debug_mode else "Conservative"
                print(f"{mode_name} mode: using reliable multiprocessing...")
            def arg_iterator(num_parts):
                for min_byte, max_byte, part in _gen_byte_ranges(source_size, num_parts):
                    yield(bucketname, keyname, dest_path, split, min_byte, max_byte, max_tries, 0, parallel_processes, quiet)
            pool = multiprocessing.Pool(processes = parallel_processes)
            p = pool.map_async(_download_part, arg_iterator(num_parts))
            pool.close()
            pool.join()
            p.get(9999999)
        else:
            # Use ThreadPoolExecutor for better I/O performance
            import concurrent.futures
            try:
                if not quiet:
                    print("Using threading for optimal I/O performance...")
                    print(f"Creating {num_parts} download tasks...")
                    
                with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_processes) as executor:
                    futures = []
                    for min_byte, max_byte, part in _gen_byte_ranges(source_size, num_parts):
                        args = (bucketname, keyname, dest_path, split, min_byte, max_byte, max_tries, 0, parallel_processes, quiet)
                        future = executor.submit(_download_part, args)
                        futures.append(future)
                        if not quiet and len(futures) <= 3:  # Debug first few tasks
                            print(f"Task {len(futures)}: bytes {min_byte}-{max_byte}")
                    
                    if not quiet:
                        print(f"Submitted {len(futures)} download tasks, waiting for completion...")
                    
                    # Wait for all downloads to complete with timeout
                    try:
                        completed, not_completed = concurrent.futures.wait(futures, timeout=300)  # 5 min timeout
                        if not_completed:
                            if not quiet:
                                print(f"Warning: {len(not_completed)} tasks did not complete within timeout")
                    except Exception as wait_exc:
                        if not quiet:
                            print(f"Error waiting for tasks: {wait_exc}")
                        # Check for individual task errors
                        for i, future in enumerate(futures):
                            try:
                                future.result(timeout=1)
                            except Exception as task_exc:
                                if not quiet:
                                    print(f"Task {i+1} failed: {task_exc}")
            except ImportError:
                # Fallback to multiprocessing if threading not available
                if not quiet:
                    print("Using multiprocessing fallback...")
                def arg_iterator(num_parts):
                    for min_byte, max_byte, part in _gen_byte_ranges(source_size, num_parts):
                        yield(bucketname, keyname, dest_path, split, min_byte, max_byte, max_tries, 0, parallel_processes, quiet)
                pool = multiprocessing.Pool(processes = parallel_processes)
                p = pool.map_async(_download_part, arg_iterator(num_parts))
                pool.close()
                pool.join()
                p.get(9999999)
        if not quiet:
            print()
    t2 = time.time()-t1
    source = "S3:%s:/%s"%(bucketname, keyname)
    destination = "local:%s:%s"%(socket.gethostname(), dest_path)
    size = os.stat(dest_path).st_size
    total_time = t2
    if size == source_size:
        passed_size_check = True
    else:
        passed_size_check = False
    s3_log_append("s3_download", source, destination, size, start_time, total_time, passed_size_check, logfile="./itmi_s3lib.log")

    if not quiet:
        print("Finished downloading %0.2fM in %0.2fs (%0.2fmbps)"%(size_mb, t2, 8*size_mb/t2))



def s3_download_partial(bucketname, keyname, dest_path, quiet=False, parallel_processes=multiprocessing.cpu_count(), headers={}, guess_mimetype=True, min=None, max=None):
    conn = boto.connect_s3()
    split = 50
    max_tries = 3
    try:
        bucket = conn.get_bucket(bucketname)
    except Exception as exc:
        print("Fatal Error: source bucket %s not found, exception %s"%(bucketname, exc))
        sys.exit()
    bytes_per_chunk = split * 1024 * 1024
    key = bucket.get_key(keyname)
    if not key:
        print("Fatal Error in download: key %s not found in bucket %s"%(keyname, bucketname))
        sys.exit()
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    if '/' in dest_path and not os.path.isdir(os.path.dirname(dest_path)):
        print("Fatal Error: destination path does not exist - args %s %s %s"%(bucketname, keyname, dest_path))
        sys.exit()
    if os.path.isdir(dest_path):
        print("Fatal Error: please supply file name in directory %s to store the file"%(dest_path))
        sys.exit()
        
    max = int(max)
    min = int(min)
    source_size = max-min
    size_mb = source_size / 1024 / 1024
    t1 = time.time()
    num_parts = (size_mb+(-size_mb%split))//split
    while num_parts > 200:
        split += 5
        num_parts = (size_mb+(-size_mb%split))//split
    builtins.global_download_total = num_parts
    builtins.global_download_progress = 0
    if not quiet:
        print("downloading %s (%s) in %s chunks"%(key.size, size_mb, num_parts))

    resp = conn.make_request("HEAD", bucket=bucket, key=key)
    if resp is None:
        raise ValueError("s3 response is invalid for bucket %s key %s"%(bucket, key))
    fd = os.open(dest_path, os.O_CREAT)
    os.close(fd)
    print(source_size, num_parts)

    def arg_iterator(num_parts):
        for min_byte, max_byte, part in _gen_byte_ranges(source_size, num_parts):
            if max_byte > max:
                break
            min_byte += min
            max_byte += max
            yield(conn, bucket.name, key.name, dest_path, split, min_byte, max_byte, max_tries, 0, parallel_processes, quiet)
    pool = multiprocessing.Pool(processes = parallel_processes)
    p = pool.map_async(_download_part, arg_iterator(num_parts))
    pool.close()
    pool.join()
    p.get(9999999)
    if not quiet:
        print()

    t2 = time.time()-t1
    source = "S3:%s:/%s"%(bucketname, keyname)
    destination = "local:%s:%s"%(socket.gethostname(), dest_path)
    size = os.stat(dest_path).st_size
    total_time = t2
    passed_size_check=True
    #if size == source_size:
    #    passed_size_check = True
    #else:
    #    passed_size_check = False
    s3_log_append("s3_download", source, destination, size, start_time, total_time, passed_size_check, logfile="./itmi_s3lib.log")

    if not quiet:
        print("Finished downloading %0.2fM in %0.2fs (%0.2fmbps)"%(size_mb, t2, 8*size_mb/t2))


def s3_log_append(type, source, destination, size, start_time, total_time, passed_size_check, logfile="./transferlib.log"):
    f = None
    if os.path.isfile(logfile):
        f = open(logfile, 'a')
    else:
        f = open(logfile, 'w')
    h_size = 0
    if size > 1024*1024*1024:
        h_size = "%0.2fGb"%(size/(0.0+1024*1024*1024))
    elif size > 1024*1024:
        h_size = "%0.2fMb"%(size/(0.0+1024*1024))
    elif size > 1024:
        h_size = "%0.2fkb"%(size/(0.0+1024))
    else:
        h_size = "%0.2f"%(size)
    outlines = []
    outlines.append("%s\n"%(type))
    outlines.append("\tsource: %s\n"%(source))
    outlines.append("\tdestination: %s\n"%(destination))
    outlines.append("\tbytes transferred: %s (%s)\n"%(size, h_size))
    outlines.append("\tstart time: %s\n"%(start_time))
    outlines.append("\ttotal time: %0.3f\n"%(total_time))
    outlines.append("\tmbps: %0.2f\n"%(((size*8)/total_time)/1024/1024))
    outlines.append("\tpassed size check: %s\n"%(passed_size_check))
    f.writelines(outlines)
    f.close()
    



def compare_md5_local_to_s3(local_file_name, original_bucket, original_loc):
    if not os.path.isfile(local_file_name):
        print("comparing s3 and local md5s. file %s could not be found, exiting"%(local_file_name))

    (fd, fname) = tempfile.mkstemp()
    os.system("md5sum %s > %s"%(local_file_name, fname))
    f = os.fdopen(fd, 'r')
    checksum = None

    for line in f:
        tokens = line.rstrip('\r\n').split()
        checksum = tokens[0]
        break

    conn = boto.connect_s3()
    bucket = conn.get_bucket(original_bucket)
    key = bucket.get_key(original_loc)
    remote_md5 = None
    if not key:
        print("comparing s3 and local md5s. key %s not found in bucket %s"%(original_loc, original_bucket))
    else:
        try:
            remote_md5 = key.etag[1:-1]
        except TypeError:
            print("no md5 found for file %s bucket %s"%(original_loc, original_bucket))
    f.close()
    os.remove(fname)
    if checksum != remote_md5:
        print("fail in local md5 compare. local %s, s3 %s"%(checksum, remote_md5)) 
        return False
    else:
        return True

def compare_md5_s3_to_s3(dest_bucket, dest_key, orig_bucket, orig_key):
    conn = boto.connect_s3()
    dbucket = conn.get_bucket(dest_bucket)
    dkey = dbucket.get_key(dest_key)
    if not dkey:
        print("comparing s3 md5s. key %s not found in bucket %s"%(dest_key, dest_bucket))
        return False

    obucket = conn.get_bucket(orig_bucket)
    okey = obucket.get_key(orig_key)
    if not okey:
        print("comparing s3 md5s. key %s not found in bucket %s"%(orig_key, orig_bucket))
        return False

    try:
        dmd5 = dkey.etag[1:-1]
    except TypeError:
        print("comparing s3 md5s. no md5 found for file %s bucket %s"%(dest_key, dest_bucket))
        return False

    try:
        omd5 = okey.etag[1:-1]
    except TypeError:
        print("comparing s3 md5s. no md5 found for file %s bucket %s"%(orig_key, orig_bucket))
        return False

    if dmd5 != omd5:
        return False
    else:
        return True
    


def fetch_vendor_md5(vendor_bucket, batch, sample):
    print("fetch_vendor_md5 not yet implemented")
    return None

