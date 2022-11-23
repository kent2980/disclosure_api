from disclosure_api.taxonomy import CreateTaxonomy

def test_create_attachment_tsv():
    assert CreateTaxonomy(output_path="./doc/tests/").attachment_taxonomy_tsv() == True
    
def test_create_summary_tsv():
    assert CreateTaxonomy(output_path="./doc/tests/").summary_taxonomy_tsv() == True