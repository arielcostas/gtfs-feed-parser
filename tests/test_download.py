import os
import tempfile
import json
from unittest.mock import patch, MagicMock
from src.download import download_feed_from_url, _load_metadata, _save_metadata, _check_if_modified

def test_metadata_storage_and_loading():
    """Test that metadata can be saved and loaded correctly"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Test saving metadata
        _save_metadata(temp_dir, "test-etag-123", "Wed, 21 Jan 2025 10:00:00 GMT")
        
        # Test loading metadata
        metadata = _load_metadata(temp_dir)
        
        assert metadata is not None
        assert metadata["etag"] == "test-etag-123"
        assert metadata["last_modified"] == "Wed, 21 Jan 2025 10:00:00 GMT"

def test_metadata_file_path():
    """Test that metadata file is created in the correct location"""
    with tempfile.TemporaryDirectory() as temp_dir:
        _save_metadata(temp_dir, "test-etag", "test-date")
        
        metadata_path = os.path.join(temp_dir, '.gtfsmetadata')
        assert os.path.exists(metadata_path)
        
        with open(metadata_path, 'r') as f:
            data = json.load(f)
            assert data["etag"] == "test-etag"
            assert data["last_modified"] == "test-date"

def test_no_metadata_file():
    """Test behavior when no metadata file exists"""
    with tempfile.TemporaryDirectory() as temp_dir:
        metadata = _load_metadata(temp_dir)
        assert metadata is None

@patch('src.download.requests.head')
def test_check_if_modified_304_response(mock_head):
    """Test that 304 response is handled correctly"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save some existing metadata
        _save_metadata(temp_dir, "existing-etag", "existing-date")
        
        # Mock 304 response
        mock_response = MagicMock()
        mock_response.status_code = 304
        mock_head.return_value = mock_response
        
        is_modified, etag, last_modified = _check_if_modified("http://example.com/feed.zip", temp_dir)
        
        assert not is_modified
        assert etag == "existing-etag"
        assert last_modified == "existing-date"

@patch('src.download.requests.head')
def test_check_if_modified_200_response(mock_head):
    """Test that 200 response indicates modification"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save some existing metadata
        _save_metadata(temp_dir, "old-etag", "old-date")
        
        # Mock 200 response with new headers
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {
            'ETag': 'new-etag',
            'Last-Modified': 'new-date'
        }
        mock_head.return_value = mock_response
        
        is_modified, etag, last_modified = _check_if_modified("http://example.com/feed.zip", temp_dir)
        
        assert is_modified
        assert etag == "new-etag"
        assert last_modified == "new-date"

@patch('src.download.requests.get')
@patch('src.download.requests.head')
def test_download_with_force_flag(mock_head, mock_get):
    """Test that force download bypasses conditional checks"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save some existing metadata
        _save_metadata(temp_dir, "existing-etag", "existing-date")
        
        # Mock responses
        mock_head_response = MagicMock()
        mock_head_response.status_code = 304
        mock_head.return_value = mock_head_response
        
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.content = b'fake zip content'
        mock_get_response.headers = {}
        mock_get.return_value = mock_get_response
        
        # Mock zipfile to avoid actual extraction
        with patch('src.download.zipfile.ZipFile'):
            with patch('src.download.tempfile.mkdtemp', return_value='/tmp/fake'):
                with patch('src.download.os.remove'):
                    result = download_feed_from_url("http://example.com/feed.zip", temp_dir, force_download=True)
        
        # With force_download=True, the download should proceed even if server returns 304
        # The head request should not be called because we're forcing download
        mock_head.assert_not_called()
        mock_get.assert_called_once()
        assert result == '/tmp/fake'