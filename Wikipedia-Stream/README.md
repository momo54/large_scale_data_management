# large scale data management

Wikipedia has a stream of update events.

For example an event looks like
```
{
    "$schema": "/mediawiki/recentchange/1.0.0",
    "meta": {
        "uri": "https://id.wikipedia.org/wiki/Kategori:Artikel_biografi_September_2022",
        "request_id": "f27e8464-7580-4702-a731-efe2346f9d82",
        "id": "e2ea0687-c788-4565-810a-a924d3f63480",
        "dt": "2022-10-27T12:22:49Z",
        "domain": "id.wikipedia.org",
        "stream": "mediawiki.recentchange",
        "topic": "eqiad.mediawiki.recentchange",
        "partition": 0,
        "offset": 4264435691
    },
    "id": 53018300,
    "type": "categorize",
    "namespace": 14,
    "title": "Kategori:Artikel biografi September 2022",
    "comment": "[[:Diana, Putri Wales]] dihapus dari kategori",
    "timestamp": 1666873369,
    "user": "Ramadhan Rizkyyyy 1911",
    "bot": false,
    "server_url": "https://id.wikipedia.org",
    "server_name": "id.wikipedia.org",
    "server_script_path": "/w",
    "wiki": "idwiki",
    "parsedcomment": "<a href=\"/wiki/Diana,_Putri_Wales\" title=\"Diana, Putri Wales\">Diana, Putri Wales</a> dihapus dari kategori"
}
```

## Running

I wrote a program `main.py' that perform streaming queries over the stream of wikipedia events.
To run it:

```
pip install -r requirements.txt
python main.py
```

I allows to know the most active user.

## Todo

I want to know the mostly edited wikipedia page by a human within the last hour.