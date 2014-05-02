def generate_record_info(content_source,
                         origin_msg):
    return {u'languageOfCataloging': u'http://id.loc.gov/vocabulary/iso639-1/en',
            u'recordContentSource': content_source,
            u'recordCreationDate': datetime.datetime.utcnow().isoformat(),
            u'recordOrigin': origin_msg}
