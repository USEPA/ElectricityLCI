import datetime
import pytz
import logging as log
import math
import uuid

from typing import Optional

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
            category_path = _val(d, 'category', default='')
            location_code = _val(d, 'location', 'name', default='')
            process.id = _uid(olca.ModelType.PROCESS,
                              category_path, location_code, process.name)
            process.category = _category(
                category_path, olca.ModelType.PROCESS, writer, created_ids)
            process.description = _val(d, 'description')
            process.process_type = olca.ProcessType.UNIT_PROCESS
            process.location = _location(_val(d, 'location'), writer, created_ids)
            process.process_documentation = _process_doc(
                _val(d, 'processDocumentation'), writer, created_ids)
            process.last_change = datetime.datetime.now(pytz.utc).isoformat()
            _process_dq(d, process)
            process.exchanges = []
            last_id = 0
            for e in _val(d, 'exchanges', default=[]):
                exchange = _exchange(e, writer, created_ids)
                if exchange is not None:
                    last_id += 1
                    exchange.internal_id = last_id
                    process.exchanges.append(exchange)
            writer.write(process)

def _process_dq(d: dict, process: olca.Process):
    process.dq_entry = _format_dq_entry(
        _val(d, 'processDocumentation', 'dqEntry'))
    pdq_uid = _val(d, 'processDocumentation', 'dqSystem', '@id')
    if isinstance(pdq_uid, str):
        process.dq_system = olca.ref(olca.DqSystem, pdq_uid)
    edq_uid = _val(d, 'processDocumentation', 'exchangeDqSystem', '@id')
    if isinstance(edq_uid, str):
        process.exchange_dq_system = olca.ref(olca.DqSystem, edq_uid)


def _category(path: str, mtype: olca.ModelType, writer: pack.Writer,
              created_ids: set) -> Optional[olca.Ref]:
    if not isinstance(path, str):
        return None
    if path.strip() == '':
        return None
    parts = path.split('/')
    parent = None  # type: olca.Ref
    for i in range(0, len(parts)):
        uid_path = [str(mtype.value)] + parts[0:(i + 1)]
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


def _exchange(d: dict, writer: pack.Writer,
              created_ids: set) -> Optional[olca.Exchange]:
    if d is None:
        return None
    e = olca.Exchange()
    e.input = _val(d, 'input', default=False)
    e.quantitative_reference = _val(d, 'quantitativeReference', default=False)
    e.avoided_product = _val(d, 'avoidedProduct', default=False)
    e.amount = _val(d, 'amount', default=0.0)
    unit_name = _val(d, 'unit', 'name')
    e.unit = _unit(unit_name)
    flowprop = _flow_property(unit_name)
    e.flow_property = flowprop
    e.flow = _flow(_val(d, 'flow'), flowprop, writer, created_ids)
    e.dq_entry = _format_dq_entry(_val(d, 'dqEntry'))
    e.uncertainty = _uncertainty(_val(d, 'uncertainty'))
    e.default_provider = _process_ref(_val(d, 'provider'))
    e.description = _val(d, 'comment')
    return e


def _unit(unit_name: str) -> Optional[olca.Ref]:
    """ Get the ID of the openLCA reference unit with the given name. """
    ref_id = None
    if unit_name == 'MWh':
        ref_id = '92e3bd49-8ed5-4885-9db6-fc88c7afcfcb'
    elif unit_name == 'MJ':
        ref_id = '52765a6c-3896-43c2-b2f4-c679acf13efe'
    elif unit_name == 'kg':
        ref_id = '20aadc24-a391-41cf-b340-3e4529f44bde'
    elif unit_name == 'sh tn':
        ref_id = 'd4922696-9c95-4b5d-a876-425e98276978'
    elif unit_name == 'bbl':
        ref_id = '91995a9e-3cb4-4fc9-a93b-8c618ff9b948'
    elif  unit_name == 'cu ft':
        ref_id = '07973a41-56b3-4e1b-a208-fd75a09fbd4b'
    elif unit_name == 'btu':
        ref_id = '55244053-94ba-404e-9172-cb279d905e00'
    elif unit_name == 'lb*mi':
        ref_id = '5458351a-f6f7-4e0b-a449-823c9b6374db'
    if ref_id is None:
        log.error('unknown unit %s; no unit reference', unit_name)
        return None
    return olca.ref(olca.Unit, ref_id, unit_name)


def _flow_property(unit_name: str) -> Optional[olca.Ref]:
    """ Get the ID of the openLCA reference flow property for the unit with
        the given name.
    """
    if unit_name == 'MWh':
        ref_id = 'f6811440-ee37-11de-8a39-0800200c9a66'
        return olca.ref(olca.FlowProperty, ref_id, 'Energy')
    elif unit_name == 'MJ':
        ref_id = 'f6811440-ee37-11de-8a39-0800200c9a66'
        return olca.ref(olca.FlowProperty, ref_id, 'Energy')
    elif unit_name == 'kg':
        ref_id = '93a60a56-a3c8-11da-a746-0800200b9a66'
        return olca.ref(olca.FlowProperty, ref_id, 'Mass')
    elif unit_name == 'sh tn':
        ref_id = '93a60a56-a3c8-11da-a746-0800200b9a66'
        return olca.ref(olca.FlowProperty, ref_id, 'Mass')
    elif unit_name == 'bbl':
        ref_id = '93a60a56-a3c8-22da-a746-0800200c9a66'
        return olca.ref(olca.FlowProperty, ref_id, 'Volume')
    elif  unit_name == 'cu ft':
        ref_id = '93a60a56-a3c8-22da-a746-0800200c9a66'
        return olca.ref(olca.FlowProperty, ref_id, 'Volume')
    elif unit_name == 'btu':
        ref_id = 'f6811440-ee37-11de-8a39-0800200c9a66'
        return olca.ref(olca.FlowProperty, ref_id, 'Energy')
    elif unit_name == 'lb*mi':
        ref_id = '838aaa20-0117-11db-92e3-0800200c9a66'
        return olca.ref(olca.FlowProperty, ref_id, 'Goods transport (mass*distance)')
    log.error('unknown unit %s; no flow property reference', unit_name)
    return None


def _flow(d: dict, flowprop: olca.Ref, writer: pack.Writer,
          created_ids: set) -> Optional[olca.Ref]:
    if not isinstance(d, dict):
        return None
    uid = _val(d, 'id')
    name = _val(d, 'name')
    if isinstance(uid, str) and uid != '':
        return olca.ref(olca.Flow, uid, name)
    category_path = _val(d, 'category', default='')
    uid = _uid(olca.ModelType.FLOW, category_path, name)
    if uid not in created_ids:
        flow = olca.Flow()
        flow.id = uid
        flow.name = name
        flow.flow_type = olca.FlowType[_val(
            d, 'flowType', default='ELEMENTARY_FLOW')]
        #Do not assign flows a location
        #flow.location = _location(_val(d, 'location'),
        #                          writer, created_ids)
        flow.category = _category(category_path, olca.ModelType.FLOW,
                                  writer, created_ids)
        propfac = olca.FlowPropertyFactor()
        propfac.conversion_factor = 1.0
        propfac.flow_property = flowprop
        propfac.reference_flow_property = True
        flow.flow_properties = [propfac]
        writer.write(flow)
        created_ids.add(uid)
    return olca.ref(olca.Flow, uid, name)

def _location(d: dict, writer: pack.Writer,
              created_ids: set) -> Optional[olca.Ref]:
    code = _val(d, 'name')
    if not isinstance(code, str):
        return None
    if code == '':
        return None
    uid = _val(d, 'id')
    if isinstance(uid, str) and uid != '':
        return olca.ref(olca.Location, uid, code)
    uid = _uid(olca.ModelType.LOCATION, code)
    if uid in created_ids:
        return olca.ref(olca.Location, uid, code)
    location = olca.Location()
    location.id = uid
    location.name = code
    writer.write(location)
    created_ids.add(uid)
    return olca.ref(olca.Location, uid, code)


def _process_doc(d: dict, writer: pack.Writer,
                 created_ids: set) -> olca.ProcessDocumentation:
    doc = olca.ProcessDocumentation()
    doc.creation_date = datetime.datetime.now(pytz.utc).isoformat()
    if not isinstance(d, dict):
        return doc
    # copy the fields that have the same format as in the olca-schema spec.
    copy_fields = [
        'timeDescription',
        'technologyDescription',
        'dataCollectionDescription',
        'completenessDescription',
        'dataSelectionDescription',
        'reviewDetails',
        'dataTreatmentDescription',
        'inventoryMethodDescription',
        'modelingConstantsDescription',
        'samplingDescription',
        'restrictionsDescription',
        'copyright',
        'intendedApplication',
        'projectDescription'
    ]
    doc.from_json({field: _val(d, field) for field in copy_fields})
    doc.valid_from = _format_date(_val(d, 'validFrom'))
    doc.valid_until = _format_date(_val(d, 'validUntil'))
    doc.reviewer = _actor(_val(d, 'reviewer'), writer, created_ids)
    doc.data_documentor = _actor(_val(d, 'dataDocumentor'), writer, created_ids)
    doc.data_generator = _actor(_val(d, 'dataGenerator'), writer, created_ids)
    doc.data_set_owner = _actor(_val(d, 'dataSetOwner'), writer, created_ids)
    doc.publication = _source(_val(d, 'publication'), writer, created_ids)
    return doc


def _actor(name: str, writer: pack.Writer,
           created_ids: set) -> Optional[olca.Ref]:
    if not isinstance(name, str) or name == '':
        return None
    uid = _uid(olca.ModelType.ACTOR, name)
    if uid in created_ids:
        return olca.ref(olca.Actor, uid, name)
    actor = olca.Actor()
    actor.id = uid
    actor.name = name
    writer.write(actor)
    created_ids.add(uid)
    return olca.ref(olca.Actor, uid, name)


def _source(name: str, writer: pack.Writer,
            created_ids: set) -> Optional[olca.Ref]:
    if not isinstance(name, str) or name == '':
        return None
    uid = _uid(olca.ModelType.SOURCE, name)
    if uid in created_ids:
        return olca.ref(olca.Source, uid, name)
    source = olca.Source()
    source.id = uid
    source.name = name
    writer.write(source)
    created_ids.add(uid)
    return olca.ref(olca.Source, uid, name)


def _val(d: dict, *path, **kvargs):
    if d is None or path is None:
        return None
    v = d
    for p in path:
        if not isinstance(v, dict):
            return None
        if p not in v:
            v = None
        else:
            v = v[p]
    if v is None and 'default' in kvargs:
        return kvargs['default']
    return v


def _uncertainty(d: dict) -> Optional[olca.Uncertainty]:
    if not isinstance(d, dict):
        return None
    dt = _val(d, 'distributionType')
    if dt != 'Logarithmic Normal Distribution':
        # no other distribution type is currently used
        return None
    gmean = _val(d, 'geomMean')
    if isinstance(gmean, str):
        gmean = float(gmean)
    gsd = _val(d, 'geomSd')
    if isinstance(gsd, str):
        gsd = float(gsd)
    if not _isnum(gmean) or not _isnum(gsd):
        return None
    u = olca.Uncertainty()
    u.distribution_type = olca.UncertaintyType.LOG_NORMAL_DISTRIBUTION
    u.geom_mean = gmean
    u.geom_sd = gsd
    return u


def _process_ref(d: dict) -> Optional[olca.ProcessRef]:
    if not isinstance(d, dict):
        return None
    pr = olca.ProcessRef()
    pr.name = _val(d, 'name')
    category_path = _val(d, 'categoryPath')
    location_code = _val(d, 'location', default='')
    if isinstance(category_path, list):
        category_path = '/'.join(category_path)
    pr.id = _uid(olca.ModelType.PROCESS,
                 category_path, location_code, pr.name)
    pr.location = location_code
    pr.process_type = olca.ProcessType.UNIT_PROCESS
    return pr


def _isnum(n) -> bool:
    if not isinstance(n, (float, int)):
        return False
    return not math.isnan(n)


def _format_dq_entry(entry: str) -> Optional[str]:
    """ The data quality entries may contain floating point numbers which are
        converted to integer numbers in this function."""
    if not isinstance(entry, str):
        return None
    e = entry.strip()
    if len(e) < 2:
        return None
    nums = e[1:(len(e) - 1)].split(';')
    for i in range(0, len(nums)):
        if nums[i] == 'n.a.':
            continue
        nums[i] = str(round(float(nums[i])))
    return '(%s)' % ';'.join(nums)


def _format_date(entry: str) -> Optional[str]:
    """ Date entries currently have the format `month/day/year`. This function
        converts such entries into the ISO format year-month-day. """
    if not isinstance(entry, str):
        return None
    parts = entry.split('/')
    if len(parts) < 3:
        return None
    month = int(parts[0])
    day = int(parts[1])
    year = int(parts[2])
    return datetime.date(year, month, day).isoformat()


def _uid(*args):
    path = '/'.join([str(arg).strip() for arg in args]).lower()
    return str(uuid.uuid3(uuid.NAMESPACE_OID, path))
