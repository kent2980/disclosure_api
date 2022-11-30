from disclosure_api.tdnet_request import TdnetRequest
 
def test_request():
    assert TdnetRequest().getXBRL_link() > 0