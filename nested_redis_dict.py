import rapidjson as json
from redis import StrictRedis
import ast
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import time
import warnings
import uuid
import sys

SENTINEL = object()

class NestedRedisDict(dict):


    def __init__(self, *args, **kwargs):
        self.tmp_map = {}
        self.namespace = kwargs.pop('namespace', 'record')
        self.tuple_namespace = 'tuple_{}'.format(self.namespace)
        self.dict_key_namespace = 'dict_key_{}'.format(self.namespace)
        self.key_convert_namespace = 'key_convert_{}'.format(self.namespace)
        self.expire = kwargs.pop('expire', None)


        self.redis = StrictRedis(decode_responses=True)

        self.iter = self.iterkeys()

        result = self.redis.execute_command('JSON.GET', self.namespace, '.')
        if result == None:
            self.redis.execute_command('JSON.SET', self.namespace, '.', json.dumps({}))
        self.update(*args, **kwargs)


    def __missing__(self, key):
        raise KeyError(key.split('.')[-1])

    def _delete(self, key):
        self.redis.json().delete(self.namespace, key)

    def __delitem__(self, key):
        if '_prev_key' in self.tmp_map:
            key = '{}.{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'), key)
            dict.__delitem__(self.tmp_map, '_prev_key')
        self._delete(key=key)

    def __contains__(self, key):
        #print('The key to check', key)
        if isinstance(key, tuple):
            # print(type(key), key)
            elems = [str(elem).replace('.', '_') for elem in key]
            key = "_".join(elems)
        elif isinstance(key, str) and '.' in key:
            key = key.replace('.', '_')
        elif isinstance(key, float):
            key = str(key).replace('.', '_')

        if dict.__contains__(self.tmp_map, '_prev_key'):
            key = '{}.{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'), key)
            dict.__delitem__(self.tmp_map, '_prev_key')

        #print('To check', key)
        if self.redis.execute_command('JSON.TYPE', self.namespace, '.{}'.format(key)) == None:
            return False
        #return dict.__contains__(self, key)
        return True


    def __len__(self):
        key = ''
        if dict.__contains__(self.tmp_map, '_prev_key'):
            key = '{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'))
            dict.__delitem__(self.tmp_map, '_prev_key')

        #print('THE key {}'.format(key))
        return self.redis.json().objlen(self.namespace, '.{}'.format(key))

    def _key_exist(self, key):
        result = self.redis.execute_command('JSON.TYPE', self.namespace, '.{}'.format(key))
        if result == None:
            return False
        else:
            return True

    def append(self, val):
        if dict.__contains__(self.tmp_map, '_prev_key'):
            key = '{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'))
            dict.__delitem__(self.tmp_map, '_prev_key')
            print('Step 1')
            _, obj = self._load(key)
            print('Step 2')
            if not isinstance(obj, list):
                raise AttributeError("'NestedRedisDict' object has no attribute 'append'")
            print('Step 3', val)
            if isinstance(val, (dict, list, set)):
                val = self._dict_encoder(val, prev_key=key)
            obj.append(val)
            print('Step 4')
            self._store(key, obj)
        else:
            raise AttributeError("'NestedRedisDict' object has no attribute 'append'")


    def _store(self, key, value):
        #print('Set key', key)
        store_type = type(value).__name__

        #value = self.pre_transform.get(store_type, lambda x: x)(value)
        if isinstance(value, dict) or isinstance(value, list):
            self._cache_dict_key(key, name_space=self.dict_key_namespace)

        keys = key.split('.')

        parent_type = self.redis.execute_command('JSON.TYPE', self.namespace, '.{}'.format('.'.join(keys[:-1])))
        if parent_type == None:
            for index in range(len(keys)-1):
                test_key = '.'.join(keys[:index+1])
                #print('.{}'.format(test_key))
                try:
                    item = ast.literal_eval(keys[:index + 1][-1])
                    if isinstance(item, tuple):
                        elems = [str(elem).replace('.', '_') for elem in item]
                        keys[index] = "_".join(elems)
                        test_key = '.'.join(keys[:index + 1])
                        # self.redis.set(self.tuple_namespace, keys[index], ex=self.expire)
                        #print(test_key)
                except ValueError:
                    pass
                result = self.redis.execute_command('JSON.TYPE', self.namespace, '.{}'.format(test_key))
                #print(test_key, result, result, '.{}'.format(test_key))
                if result == None:
                    print('Create Path', test_key)
                    self._cache_dict_key(test_key, name_space=self.dict_key_namespace)
                    self.redis.execute_command('JSON.SET', self.namespace, '.{}'.format(test_key), json.dumps({}))
                    #rprint('> Create <', self.redis.json().get(self.namespace))
        elif parent_type != 'object':
            parent_path = '.'.join(keys[:-1])
            self._cache_dict_key(parent_path, name_space=self.dict_key_namespace)
            self.redis.execute_command('JSON.SET', self.namespace, '.{}'.format(parent_path), json.dumps({}))

        try:
            item = ast.literal_eval(keys[-1])
            if isinstance(item, tuple):
                elems = [str(elem).replace('.', '_') for elem in item]
                keys[-1] = "_".join(elems)
                #self.redis.set(self.tuple_namespace, keys[-1], ex=self.expire)
        except ValueError:
            pass

        key = '.'.join(keys)
        #self.redis.set(self.tuple_namespace, keys[-1], ex=self.expire)
        #print('--->',  key, value, parent_type)
        self.redis.execute_command('JSON.SET', self.namespace, '.{}'.format(key), json.dumps(value))
        #print('-------->', self.redis.json().get(self.namespace))

    def _load(self, key, dict_decode=False):
        #if not prev_key:
        #    if key not in self:
        #        raise KeyError(key.split('.')[-1])
        a = time.time()
        result = self.redis.execute_command('JSON.GET', self.namespace, '.{}'.format(key))

        #print('Load data time', time.time()-a, type(result), '.{}'.format(key))
        #print('>>>>>>>>>', result, type(result))
        if result is None:
            return False, None
        #t, value = result.split(':', 1)
        #print('Treatment', t, 'val', value)
        try:
            a = time.time()
            decode = json.loads(result)
            print('decode', time.time() - a, isinstance(decode, dict), decode)
            if not dict_decode:
                return True, decode
            elif dict_decode and isinstance(decode, (dict, list, set, tuple)):
                return True, self._dict_decoder(decode, prev_key=key)
        except Exception:
            #print('\nInside here', dict_decode, result, isinstance(result, dict), key)
            if dict_decode and isinstance(result, (dict, list, set, tuple)):
                return True, self._dict_decoder(result, prev_key=key)
            return True, result

    def __repr__(self):
        key = ''
        if '_prev_key' in self.tmp_map:
            key = '{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'))
            dict.__delitem__(self.tmp_map, '_prev_key')
        found, val = self._load(key)
        if not found:
            raise KeyError(key)

        val = self._dict_decoder(val, prev_key=key)
        return str(val)

    def iteritems(self):
        """Note: for pythone2 str is needed"""
        for item in self.iterkeys():
            prev_key = ''
            key = item
            if isinstance(key, tuple):
                #print(type(key), key)
                elems = [str(elem).replace('.', '_') for elem in key]
                key = "_".join(elems)
            elif isinstance(key, str) and '.' in key:
                print('inside')
                key = key.replace('.', '_')
            elif isinstance(key, float):
                key = str(key).replace('.', '_')

            if '_prev_key' in self.tmp_map:
                prev_key = '{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'))
                key = '{}.{}'.format(prev_key, key)

            #print('To load', key, prev_key)
            yield str(item), self._load(key, dict_decode=True)[1]

            #if prev_key and len(prev_key) > 0:
            #    self.tmp_map['_prev_key'] = prev_key

    def values(self):
        return list(self.itervalues())

    def itervalues(self):
        for item in self.iterkeys():
            prev_key = ''
            key = item
            if isinstance(key, tuple):
                # print(type(key), key)
                elems = [str(elem).replace('.', '_') for elem in key]
                key = "_".join(elems)
            elif isinstance(key, str) and '.' in key:
                print('inside')
                key = key.replace('.', '_')
            elif isinstance(key, float):
                key = str(key).replace('.', '_')

            if '_prev_key' in self.tmp_map:
                prev_key = '{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'))
                key = '{}.{}'.format(prev_key, key)

            # print('To load', key, prev_key)
            yield self._load(key, dict_decode=True)[1]

    def items(self):
        return list(self.iteritems())

    def to_dict(self):
        return dict(self.items())

    def pop(self, key, default=SENTINEL):

        if isinstance(key, tuple):
            # print(type(key), key)
            elems = [str(elem).replace('.', '_') for elem in key]
            key = "_".join(elems)
        elif isinstance(key, str) and '.' in key:
            print('inside')
            key = key.replace('.', '_')
        elif isinstance(key, float):
            key = str(key).replace('.', '_')

        if '_prev_key' in self.tmp_map:
            key = '{}.{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'), key)
            dict.__delitem__(self.tmp_map, '_prev_key')

        value = self._load(key, dict_decode=True)[1]
        self._delete(key)
        return value

    def popitem(self):
        while True:
            keys = self.keys()
            if keys is None or len(keys) == 0:
                raise KeyError("popitem(): dictionary is empty")
            try:
                return keys[-1], self.pop(keys[-1])
            except KeyError:
                continue

    def _key_decode(self, key, prev_key, eval=False):
        if '_' in key:
            if prev_key and len(prev_key):
                look_up_key = '{}.{}'.format(prev_key, key)
            else:
                look_up_key = key

            result = self._lookup_cached_dict_key(look_up_key, name_space=self.key_convert_namespace, get_type='dict')
            #print('look_up_key', look_up_key,result )
            if result:
                try:
                    return ast.literal_eval(result)
                except Exception:
                    return float(result)
            return key
        else:
            return key

    def _dict_decoder(self, val, prev_key=None):
        """
            Recursively goes through the dictionary obj and replaces keys with the convert function.
        """
        #print('\n')


        if isinstance(val, (str, int, float)):
            return val
        if isinstance(val, dict):
            new = val.__class__()
            for k, v in val.items():
                new_key = self._key_decode(k, prev_key=prev_key)
                if prev_key and len(prev_key):
                    pass_key = '{}.{}'.format(prev_key, k)
                else:
                    pass_key = k

                #print('\nCovert key', k, new_key, 'prev_key', pass_key)
                new[new_key] = self._dict_decoder(v, prev_key=pass_key)
        elif isinstance(val, (list, set, tuple)):
            new = val.__class__(self._dict_decoder(v, prev_key=prev_key) for v in val)
        else:
            return val
        return new

    def _lookup_cached_dict_key(self, key, name_space, get_type='set'):
        #print('Lookup', '{}:{}'.format(self.dict_key_namespace, key))
        #return self.redis.exists('{}:{}'.format(self.dict_key_namespace, key))
        if get_type.lower() == 'set':
            return self.redis.sismember(name_space, key)
        elif get_type.lower() == 'dict':
            return self.redis.get('{}:{}'.format(name_space, key))

    def __getitem__(self, key):
        #print('Get item', key, self.tmp_map)
        try:
            if isinstance(key, tuple):
                #print(type(key), key)
                elems = [str(elem).replace('.', '_') for elem in key]
                key = "_".join(elems)
            elif isinstance(key, str) and '.' in key:
                key = key.replace('.', '_')
            elif isinstance(key, float):
                key = str(key).replace('.', '_')
        except ValueError:
            pass
        #print(key, type(key))
        if '_prev_key' in self.tmp_map:
            key = '{}.{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'), key)
            #print('Delete prev_key', key)
            dict.__delitem__(self.tmp_map, '_prev_key')

        if not self._key_exist(key):
            raise KeyError(key)

        a = time.time()
        result = self._lookup_cached_dict_key(key, name_space=self.dict_key_namespace)

        if not result:
            found, val = self._load(key)
            print('dict key lookup time', time.time() - a, key, result, found, val)
            if not found:
                raise KeyError(key)

            print('Return value', key, val)
            return val
        else:
            if 'prev_key' in self.tmp_map:
                self.tmp_map['_prev_key'] = '{}.{}'.format(self.tmp_map['_prev_key'], key)
                #print('Returnning handle only!', self.tmp_map['_prev_key'])
            else:
                self.tmp_map['_prev_key'] = key

            #print('Return handle', self.tmp_map['_prev_key'])
            return self


    def _cache_dict_key(self, key, name_space, val='dict', add_type='set'):
        #print('cached', '{}:{}'.format(self.dict_key_namespace, key))
        #self.redis.set('{}:{}'.format(self.dict_key_namespace, key), 'dict', ex=self.expire)
        if add_type.lower() == 'set':
            self.redis.sadd(name_space, key)
        elif add_type.lower() == 'dict':
            self.redis.set('{}:{}'.format(name_space, key), val, ex=self.expire)

    def _key_encode(self, key, prev_key):
        if isinstance(key, tuple):
            elems = [str(elem).replace('.', '_') for elem in key]
            new_key = "_".join(elems)

        elif isinstance(key, str) and '.' in key:
            new_key = key.replace('.', '_')

        elif isinstance(key, float):
            new_key = str(key).replace('.', '_')

        else:
            return str(key)  # Encode all keys as str

        self._cache_dict_key('{}.{}'.format(prev_key, new_key), name_space=self.key_convert_namespace,
                             val='{}'.format(key), add_type='dict')
        return new_key

    def _dict_encoder(self, val, prev_key=None):
        """
            Recursively goes through the dictionary obj and replaces keys with the convert function.
        """
        if isinstance(val, (str, int, float)):
            return val

        if isinstance(val, dict):
            new = val.__class__()
            for k, v in val.items():
                new_key = self._key_encode(k, prev_key)
                new[new_key] = self._dict_encoder(v, prev_key='{}.{}'.format(prev_key, new_key))
        elif isinstance(val, (list, tuple)):
            new = val.__class__(self._dict_encoder(v, prev_key=prev_key) for v in val)
        elif isinstance(val, set):
            #print('prev', prev_key)
            new = [self._dict_encoder(v, prev_key=prev_key) for v in val]
        else:
            return val
        return new

    def iter_keys(self):
        """Deprecated: should be removed after major version change"""
        print("Warning: deprecated method. use iterkeys instead")
        return self.iterkeys()

    def iterkeys(self):
        prev_key = ''
        if '_prev_key' in self.tmp_map:
            prev_key = '{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'))

        for key in self.redis.json().objkeys('record', '.{}'.format(prev_key)):
            #print('Encode', key, prev_key)
            yield self._key_decode(key, prev_key, eval=True)

        if '_prev_key' in self.tmp_map:
            dict.__delitem__(self.tmp_map, '_prev_key')

    def keys(self):
        return list(self.iterkeys())

    def clear(self):
        self.redis.execute_command('DEL', self.namespace, '.')
        self.redis.execute_command('JSON.SET', self.namespace, '.', json.dumps({}))

    def __iter__(self):
        self.iter = self.iterkeys()
        return self

    def __next__(self):
        return next(self.iter)

    def __setitem__(self, key, val):
        #print('To set key', key,'Val', type(val))
        key_before_modify = None
        if not isinstance(key, int):
            key_split = key.split(':')
            if key_split[0] != 'PROCESSED':
                if isinstance(key, tuple):
                    # print(type(key), key)
                    key_before_modify = key
                    elems = [str(elem).replace('.', '_') for elem in key]
                    key = "_".join(elems)
                elif isinstance(key, str) and '.' in key:
                    key_before_modify = key
                    key = key.replace('.', '_')
            else:
                key = ':'.join(key_split[1:])
        else:
            key = str(key)

        if '_prev_key' in self.tmp_map:
            #print('Delete prev_key', key, dict.__getitem__(self.tmp_map, '_prev_key'))
            key = '{}.{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'), key)
            dict.__delitem__(self.tmp_map, '_prev_key')

        if key_before_modify:
            #print('Modified key', key, key_before_modify)
            self._cache_dict_key(key, name_space=self.key_convert_namespace, val='{}'.format(key_before_modify),
                                 add_type='dict')

        if isinstance(val, dict):
            #print('In setitem')
            print('Item is dict', key, self._key_exist(key))
            if self._key_exist(key):
                NestedRedisDict(val, prev_key=key)  # Prevent override the child dict, in case only changing values
            else:
                print('Large dumps', key)
                val = self._dict_encoder(val, prev_key=key)
                self._store(key, val)  # For large dataset dumping
        else:
            #print('True set value', key, 'Val', val, type(val))
            #dict.__setitem__(self, key, val)
            self._store(key, val)  # Design for setting values for some specific key path

    def _import_worker(self, data):
        try:
            key_before_modify = data['key']
            if isinstance(data['key'], tuple):
                # print(type(key), key)
                elems = [str(elem).replace('.', '_') for elem in data['key']]
                data['key'] = "_".join(elems)
                self._cache_dict_key(data['key'], name_space=self.key_convert_namespace,
                                    val='{}'.format(key_before_modify),
                                    add_type='dict')
            elif isinstance(data['key'], str) and '.' in data['key']:
                data['key'] = data['key'].replace('.', '_')
                self._cache_dict_key(data['key'], name_space=self.key_convert_namespace,
                                     val='{}'.format(key_before_modify),
                                     add_type='dict')
            elif isinstance(data['key'], float):
                data['key'] = str(data['key']).replace('.', '_')
                self._cache_dict_key(data['key'], name_space=self.key_convert_namespace,
                                     val='{}'.format(key_before_modify),
                                     add_type='dict')

        except ValueError:
            pass
        k = 'PROCESSED:{}.{}'.format(data['prev_key'], data['key'])
        self[k] = data['value']

    def setdefault(self, key, default_value=None):

        if isinstance(key, tuple):
            # print(type(key), key)
            elems = [str(elem).replace('.', '_') for elem in key]
            lookup_key = "_".join(elems)
        elif isinstance(key, str) and '.' in key:
            lookup_key = key.replace('.', '_')
        elif isinstance(key, float):
            lookup_key = str(key).replace('.', '_')
        else:
            lookup_key = key

        if '_prev_key' in self.tmp_map:
            lookup_key = '{}.{}'.format(dict.__getitem__(self.tmp_map, '_prev_key'), lookup_key)

        if not self._key_exist(lookup_key):
            self[key] = default_value

        return self[lookup_key]

    @contextmanager
    def expire_at(self, sec_epoch):
        self.expire, temp = sec_epoch, self.expire
        yield
        self.expire = temp

    def __sizeof__(self):
        return self.to_dict().__sizeof__()

    def fromkeys(self, iterable, value=None, namespace=None):
        if not namespace:
            namespace = str(uuid.uuid4())
            warnings.warn('Redis namespace not provided, using namespace {}'.format(namespace))
        data = {}.fromkeys(iterable, value)
        return NestedRedisDict(data, namespace=namespace)

    def update(self, *args, **kwargs):
        prev_key = kwargs.get('prev_key', None)
        multi_process = kwargs.get('multi_process', False)
        if multi_process:
            def init_generation_func():
                for k, v in dict(*args).items():
                    yield {'key': k, 'value': v, 'prev_key': prev_key}
            generator = init_generation_func()
            with ThreadPoolExecutor(max_workers=32) as executor:
                executor.map(self._import_worker, generator)
        else:
            for k, v in dict(*args).items():
                print('To update', k, v, prev_key, type(k))
                try:
                    if isinstance(k, tuple):

                        key_before_modify = k
                        elems = [str(elem).replace('.', '_') for elem in k]
                        k = "_".join(elems)

                        if prev_key != None:
                            add_key = '{}.{}'.format(prev_key, k)
                        else:
                            add_key = k


                        #print('UPdate convert', key_before_modify, k, add_key)
                        self._cache_dict_key(add_key, name_space=self.key_convert_namespace,
                                             val='{}'.format(key_before_modify),
                                             add_type='dict')
                    elif isinstance(k, str) and '.' in k:
                        key_before_modify = k
                        k = k.replace('.', '_')

                        if prev_key != None:
                            add_key = '{}.{}'.format(prev_key, k)
                        else:
                            add_key = k

                        #print('UPdate convert', key_before_modify, k)
                        self._cache_dict_key(add_key, name_space=self.key_convert_namespace,
                                             val='{}'.format(key_before_modify),
                                             add_type='dict')
                    elif isinstance(k, float):
                        key_before_modify = k
                        k = str(k).replace('.', '_')

                        if prev_key != None:
                            add_key = '{}.{}'.format(prev_key, k)
                        else:
                            add_key = k

                        #print('UPdate convert', key_before_modify, k)
                        self._cache_dict_key(add_key, name_space=self.key_convert_namespace,
                                             val='{}'.format(key_before_modify),
                                             add_type='dict')
                except ValueError:
                    pass
                if prev_key != None:
                    k = 'PROCESSED:{}.{}'.format(prev_key, k)
                self[k] = v
