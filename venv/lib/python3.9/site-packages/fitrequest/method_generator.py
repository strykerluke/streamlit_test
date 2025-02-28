import logging
from abc import ABCMeta
from enum import Enum
from typing import List, Optional

from makefun import create_function
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class RequestMethod(str, Enum):
    delete = 'DELETE'
    get = 'GET'
    patch = 'PATCH'
    post = 'POST'
    put = 'PUT'


class MethodDetails(BaseModel):
    name: str
    endpoint: str
    docstring: Optional[str] = None
    docstring_variables: Optional[set] = None
    exec_method: str = '_request'
    extra_params: Optional[list] = None
    raise_for_status: bool = True
    request_method: RequestMethod = RequestMethod.get
    resource_name: Optional[str] = None
    response_key: Optional[str] = None


class _MethodsGenerator(ABCMeta):
    def get_and_extend_methods(cls) -> List[MethodDetails]:
        methods_extended = []
        for method in cls._methods_binding:
            if cls._docstring_template and not method.get('docstring'):
                method['docstring'] = cls._docstring_template.format(**method)
            methods_extended.append(method)
            if method.pop('create_save_method', True):
                new_method = cls._create_save_method_from(method)
                methods_extended.append(new_method)
        return [
            MethodDetails(
                **method,
            )
            for method in methods_extended
        ]

    def _create_save_method_from(cls, method: dict) -> dict:
        result = method.copy()
        prefix_verb = method['name'].split('_')[0]
        result['name'] = method['name'].replace(f'{prefix_verb}_', 'save_')
        result['exec_method'] = f'{method.get("exec_method", "_request")}_and_save'
        result['extra_params'] = method['extra_params'] + ['filepath'] if method.get('extra_params') else ['filepath']
        result['docstring'] = f"{method.get('docstring')}\nSaves the data to a file." if method.get('docstring') else ''
        return result

    def _generate_method(cls, method_details: MethodDetails) -> str:
        func_name = method_details.name.strip('_')
        func_signature = cls._generate_signature(method_details)
        func_gen = create_function(func_signature, getattr(cls, method_details.name))
        setattr(cls, func_name, func_gen)
        return getattr(cls, func_name)

    def _generate_signature(cls, method_details: MethodDetails) -> str:
        arg_separator = ': str, '
        func_signature = f'{method_details.name}(self, '
        if method_details.resource_name:
            func_signature += f'{method_details.resource_name}{arg_separator}'
        if method_details.extra_params:
            func_signature += f'{arg_separator.join(method_details.extra_params)}{arg_separator}'
        func_signature += f'params: dict = None, raise_for_status: bool = {method_details.raise_for_status}, **kwargs)'
        return func_signature

    def _get_modified_method(cls, func, method_details: MethodDetails):
        frozen_kwargs = {
            'method': method_details.request_method,
            'endpoint': method_details.endpoint,
            'response_key': method_details.response_key,
        }

        def modified_method(*args, **kwargs):
            # These parameters are defined by the methods_binding attribute and must not be passed
            if 'method' in kwargs:
                kwargs.pop('method')
                logger.warning('Method parameter is not authorized and defined in the methods binding dictionary.')
            if 'endpoint' in kwargs:
                kwargs.pop('endpoint')
                logger.warning('Endpoint parameter is not authorized and defined in the methods binding dictionary.')
            if method_details.resource_name in kwargs:
                frozen_kwargs['resource_id'] = kwargs[method_details.resource_name]
            kwargs.update(frozen_kwargs)
            return func(*args, **kwargs)

        return modified_method

    def __new__(cls, classname, supers, cls_dict):
        client = type.__new__(cls, classname, supers, cls_dict)
        if client._methods_binding:
            for method_details in client.get_and_extend_methods():
                func = getattr(client, method_details.exec_method)
                modified_func = client._get_modified_method(func, method_details)
                setattr(client, method_details.name, modified_func)
                func_name = method_details.name.strip('_')
                cls_dict[func_name] = client._generate_method(method_details=method_details)
                if method_details.docstring:
                    cls_dict[func_name].__doc__ = method_details.docstring
        return type.__new__(cls, classname, supers, cls_dict)
