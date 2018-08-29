import uuid

import olca
import olca.pack as pack


def write(processes: dict, file_path: str):
    """ Write the given process dictionary to a olca-schema zip file with the
        given path.
    """
    with pack.Writer(file_path) as writer:
        for d in processes.values():
            process = olca.Process()
            process.name = _val(d, 'name')
            category_path = _val(d, 'category')
            process.id = _uid(olca.ModelType.PROCESS,
                              category_path, process.name)
            writer.write(process)


def _val(d: dict, *path, **kvargs):
    if d is None or path is None:
        return None
    v = d
    for p in path:
        if not isinstance(v, dict):
            return None
        v = v[p]
    if v is None and 'default' in kvargs:
        return kvargs['default']
    return v


def _uid(*args):
    path = '/'.join([str(arg).strip() for arg in args]).lower()
    return str(uuid.uuid3(uuid.NAMESPACE_OID, path))


# this currently just for testing and will be removed later
if __name__ == '__main__':
    import pickle
    with open('../CAMX_gen_dict_WI.p', 'rb') as f:
        processes = pickle.load(f)
        write(processes, '../ElectricityLCI_jsonld.zip')
