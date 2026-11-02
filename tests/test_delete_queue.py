from databusclient.api.delete import DeleteQueue

def test_new_queue_is_empty():
    q = DeleteQueue("fake-key")
    assert q.is_empty() is True
    assert q.is_not_empty() is False


def test_add_uri_makes_queue_not_empty():
    q = DeleteQueue("fake-key")
    q.add_uri("https://databus.dbpedia.org/acc/group") 
    assert q.is_empty() is False


def test_add_uris_adds_all():
    q = DeleteQueue("fake-key")
    q.add_uris(["https://databus.dbpedia.org/acc/group",
               "https://databus.dbpedia.org/acc/group/artifact",
               "https://databus.dbpedia.org/acc/group/artifact/2024.01.01"
  ])
    assert len(q.queue) == 3

def test_duplicate_uri_stored_once():
    q = DeleteQueue("fake-key")
    q.add_uri("https://databus.dbpedia.org/acc/group") 
    q.add_uri("https://databus.dbpedia.org/acc/group")
    assert len(q.queue) == 1


def test_sorted_queue_deepest_first():
    q = DeleteQueue("fake-key")
    q.add_uri("https://databus.dbpedia.org/acc/group/artifact/2024.01.01") 
    q.add_uri("https://databus.dbpedia.org/acc/group/artifact")
    q.add_uri("https://databus.dbpedia.org/acc/group")
    assert q.sorted_queue() == [
        "https://databus.dbpedia.org/acc/group/artifact/2024.01.01",
        "https://databus.dbpedia.org/acc/group/artifact",
        "https://databus.dbpedia.org/acc/group",
]

def test_path_depth_counts_segments ():
   assert DeleteQueue._path_depth("https://databus.dbpedia.org/acc/group") == 2
   assert DeleteQueue._path_depth("https://databus.dbpedia.org/acc/group/artifacat/2024.01.01") == 4


def test_path_depth_ignore_trailing_slash():
    assert DeleteQueue._path_depth("https://databus.dbpedia.org/acc/group/") == 2






