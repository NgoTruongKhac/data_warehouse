# location_mapping.py

def get_location_name(key):
    """
    Hàm trả về tên địa điểm dựa trên location_key.
    """
    mapping = {
        "353981": "ho chi minh",
        "353412": "ha noi",
        "427264": "da nang"
    }
    
    # Trả về tên nếu tìm thấy, nếu không trả về 'Unknown'
    return mapping.get(str(key), "Unknown Location")