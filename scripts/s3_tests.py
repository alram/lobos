import boto3
import pytest
import random
import string
from botocore.client import Config
from botocore.exceptions import ClientError


class TestLobosS3:
    """S3 API compatibility tests for Lobos"""
    
    @classmethod
    def setup_class(cls):
        """Setup S3 client pointing to Lobos endpoint"""
        cls.endpoint_url = "http://127.0.0.1:8080" 
        cls.s3_client = boto3.client(
            's3',
            endpoint_url=cls.endpoint_url,
            aws_access_key_id='test', 
            aws_secret_access_key='test',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1',
            use_ssl=False,
            verify=False
        )
        
        # Generate random test identifiers
        cls.test_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        cls.bucket_name = f"test-bucket-{cls.test_id}"
        cls.object_key = f"test-object-{cls.test_id}"
        cls.mpu_key = f"test-mpu-{cls.test_id}"
        cls.upload_id = None
        cls.part_etag = None
    
    @pytest.mark.order(1)
    def test_list_all_my_buckets(self):
        """Test ListAllMyBuckets"""
        response = self.s3_client.list_buckets()
        assert 'Buckets' in response
        print(f"Found {len(response['Buckets'])} buckets")
    
    @pytest.mark.order(2)
    def test_create_bucket(self):
        """Test CreateBucket"""
        response = self.s3_client.create_bucket(Bucket=self.bucket_name)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        print(f"Created bucket: {self.bucket_name}")
    
    @pytest.mark.order(3)
    def test_list_bucket(self):
        """Test ListBucket (ListObjectsV2)"""
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        assert 'Name' in response
        print(f"Listed objects in {self.bucket_name}: {response.get('KeyCount', 0)} objects")
    
    @pytest.mark.order(4)
    def test_put_object(self):
        """Test PutObject"""
        test_data = b"Hello from Lobos test!"
        response = self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.object_key,
            Body=test_data
        )
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        print(f"Put object: {self.object_key}")
    
    @pytest.mark.order(5)
    def test_get_object(self):
        """Test GetObject"""
        response = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key=self.object_key
        )
        retrieved_data = response['Body'].read()
        assert len(retrieved_data) > 0
        print(f"Got object: {self.object_key}, size: {len(retrieved_data)} bytes")
    
    @pytest.mark.order(6)
    def test_mpu_create(self):
        """Test CreateMultipartUpload"""
        response = self.s3_client.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=self.mpu_key
        )
        assert 'UploadId' in response
        TestLobosS3.upload_id = response['UploadId']
        print(f"Created MPU for {self.mpu_key}, UploadId: {self.upload_id}")
    
    @pytest.mark.order(7)
    def test_mpu_upload_part(self):
        """Test UploadPart"""
        # Upload a part
        part_data = b"A" * (5 * 1024 * 1024)  # 5MB part
        response = self.s3_client.upload_part(
            Bucket=self.bucket_name,
            Key=self.mpu_key,
            PartNumber=1,
            UploadId=self.upload_id,
            Body=part_data
        )
        assert 'ETag' in response
        TestLobosS3.part_etag = response['ETag']
        print(f"Uploaded part 1, ETag: {self.part_etag}")
    
    @pytest.mark.order(8)
    def test_list_multipart_uploads(self):
        """Test ListMultipartUploads"""
        response = self.s3_client.list_multipart_uploads(Bucket=self.bucket_name)
        assert 'Uploads' in response or response.get('Uploads') is None
        print(f"Listed MPUs in {self.bucket_name}: {len(response.get('Uploads', []))} uploads")
    
    @pytest.mark.order(9)
    def test_mpu_complete(self):
        """Test CompleteMultipartUpload"""
        response = self.s3_client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=self.mpu_key,
            UploadId=self.upload_id,
            MultipartUpload={
                'Parts': [
                    {
                        'ETag': self.part_etag,
                        'PartNumber': 1
                    }
                ]
            }
        )
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        print(f"Completed MPU for {self.mpu_key}")
    
    @pytest.mark.order(10)
    def test_abort_multipart_upload(self):
        """Test AbortMultipartUpload"""
        # Create a new MPU to abort
        mpu_response = self.s3_client.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=f"{self.mpu_key}-abort"
        )
        upload_id = mpu_response['UploadId']
        
        # Abort it
        response = self.s3_client.abort_multipart_upload(
            Bucket=self.bucket_name,
            Key=f"{self.mpu_key}-abort",
            UploadId=upload_id
        )
        assert response['ResponseMetadata']['HTTPStatusCode'] == 204
        print(f"Aborted MPU {upload_id}")
    
    @pytest.mark.order(11)
    def test_delete_object(self):
        """Test DeleteObject"""
        response = self.s3_client.delete_object(
            Bucket=self.bucket_name,
            Key=self.object_key
        )
        assert response['ResponseMetadata']['HTTPStatusCode'] == 204
        print(f"Deleted object: {self.object_key}")
    
    @pytest.mark.order(12)
    def test_delete_bucket(self):
        """Test DeleteBucket"""
        # Delete the MPU object first
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=self.mpu_key)
        except ClientError:
            pass
        
        response = self.s3_client.delete_bucket(Bucket=self.bucket_name)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 204
        print(f"Deleted bucket: {self.bucket_name}")

    @pytest.mark.order(13)
    def test_delete_bucket_with_incomplete_mpu(self):
        """Test DeleteBucket with incomplete MPUs"""
        # Create a new bucket for this test
        temp_bucket = f"test-bucket-incomplete-{self.test_id}"
        response = self.s3_client.create_bucket(Bucket=temp_bucket)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        
        # Start MPU
        response = self.s3_client.create_multipart_upload(
            Bucket=temp_bucket,
            Key=self.mpu_key
        )
        assert 'UploadId' in response
        upload_id = response['UploadId']  # Use this local variable
        
        # Upload a part
        part_data = b"A" * (5 * 1024 * 1024)  # 5MB part
        response = self.s3_client.upload_part(
            Bucket=temp_bucket,
            Key=self.mpu_key,
            PartNumber=1,
            UploadId=upload_id,  # Changed from self.upload_id to upload_id
            Body=part_data
        )
        assert 'ETag' in response
        part_etag = response['ETag']
        
        # Try to delete bucket with incomplete MPU
        response = self.s3_client.delete_bucket(Bucket=temp_bucket)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 204
        print(f"Deleted bucket with incomplete parts: {temp_bucket}")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])