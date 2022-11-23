from disclosure_api.request import TdnetRequest
 
def test_request():
    assert TdnetRequest().getXBRL_link() == True