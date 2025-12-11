# s3turbo

A Python command-line program for parallel, concurrent transfer of large files to, from, and between S3 buckets and local storage.

Forked from [deaconjs/s3turbo](https://github.com/deaconjs/s3turbo).

## Installation

```bash
pip install boto3
```

Configure your AWS credentials using the AWS CLI or environment variables.

## Usage

### Download from S3

```bash
python s3turbo.py --fast s3://bucket_name/path/to/file local:///full/path/to/destination
```

### Upload to S3

```bash
python s3turbo.py --fast local:///full/path/to/file s3://bucket_name/path/to/destination
```

### Copy between S3 buckets

```bash
python s3turbo.py --fast s3://source_bucket/path/file s3://dest_bucket/path/file
```

## Optional Flags

- `--fast` - Enable fast parallel transfer mode (recommended)
- `dryrun` - Print files to be transferred without actually transferring
- `reduced_redundancy` - Use reduced redundancy AWS storage class
