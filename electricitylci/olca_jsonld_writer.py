import typing
import uuid

import olca
import olca.pack as pack


def write(processes: dict, file_path: str):
    """ Write the given process dictionary to a olca-schema zip file with the
        given path.
    """
    with pack.Writer(file_path) as writer:
        created_ids = set()
        for d in processes.values():
            process = olca.Process()
            process.name = _val(d, 'name')
            category_path = _val(d, 'category')
            process.id = _uid(olca.ModelType.PROCESS,
                              category_path, process.name)
            process.category = _category(
                category_path, olca.ModelType.PROCESS, writer, created_ids)
            # writer.write(process)


def _category(path: str, mtype: olca.ModelType, writer: pack.Writer,
              created_ids: set) -> typing.Optional[olca.Ref]:
    if not isinstance(path, str):
        return None
    if path.strip() == '':
        return None
    parts = path.split('/')
    parent = None  # type: olca.Ref
    for i in range(0, len(parts)):
        uid_path = [str(mtype.value)] + parts[0:(i+1)]
        uid = _uid(*uid_path)
        name = parts[i].strip()
        if uid not in created_ids:
            category = olca.Category()
            category.id = uid
            category.model_type = mtype
            category.name = name
            category.category = parent
            writer.write(category)
            created_ids.add(uid)
        parent = olca.ref(olca.Category, uid, name)
        parent.category_path = uid_path[1:]
    return parent


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
    import os
    import pickle
    zip_file = '../ElectricityLCI_jsonld.zip'
    if os.path.isfile(zip_file):
        os.remove(zip_file)
    with open('../CAMX_gen_dict_WI.p', 'rb') as f:
        data = pickle.load(f)
        write(data, zip_file)
