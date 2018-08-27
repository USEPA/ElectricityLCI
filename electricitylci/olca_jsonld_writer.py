import zipfile
from iomb.util import make_uuid
from iomb.olca import dump

#testing
zip_file = 'camx_json-ld.zip'
process_collection = camx_gen_dict

def write_dict_of_processes_to_olca_jsonld(process_collection,zip_file):
    #Initite a zipfile
    pack = zipfile.ZipFile(zip_file, mode='a', compression=zipfile.ZIP_DEFLATED)

    for p in process_collection:
        process = process_collection[p]
        name = process['name']
        process['@id'] = make_uuid('PROCESS',name)
        #Give each exchange a UUID
        for exchange in process['exchanges']:
           exchange['@id'] = make_uuid('EXCHANGE',exchange['flow']['name'])
        dump(process,'processes',pack)

    pack.close()


#adapted from https://github.com/USEPA/IO-Model-Builder/blob/master/iomb/olca/__init__.py
def dump(obj: dict, folder: str, pack: zipf.ZipFile):
    """ dump writes the given dictionary to the zip-file under the given folder.
    """
    uid = obj.get('@id')
    #if uid is None or uid == '':
        log.error('No @id for object %s in %s', obj, folder)
        return
    path = '%s/%s.json' % (folder, obj['@id'])
    s = json.dumps(obj)
    pack.writestr(path, s)