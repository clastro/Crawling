#1. Tor 브라우저를 설치
#2. Tor 브라우저를 컨트롤하는 패키지

url = 'youtube.com'
from torpy.http.requests import TorRequests
with TorRequests() as tor_requests:
    with tor_requests.get_session() as sess:
        response = sess.get(url)
        
# but! response.text 가 반환되는 값이 requests와 다르고 속도가 현저히 느리다는 단점
