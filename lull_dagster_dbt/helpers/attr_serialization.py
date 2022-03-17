# Reference attrs_serde:
# https://github.com/jondot/attrs-serde/blob/master/attrs_serde/attrs_serde.py

import attr
from functools import reduce


def attr_serialization(cls=None, from_key="from", from_default=[]):
    class Skipped:
        pass

    def get_field_value(field, d, ignore_default=False):
        if not field.init:
            return Skipped()

        if from_key in field.metadata:
            # 'from' is provided in the metadata

            if not isinstance(field.metadata[from_key], list):
                raise Exception(
                    "Serialization 'from' for {} must be a list of strings.".format(
                        field.name
                    )
                )

            cols = field.metadata[from_key]
        else:
            # 'from' is not provided: use the attr name
            cols = [field.name]

        # Combine with default from key:
        # If the ignore_default key is not present in the metadata
        # Or if ignore_default is present in metadata but False
        # And ignore_default isn't passed in from from_dict()
        if (
            "ignore_default" not in field.metadata
            or not field.metadata["ignore_default"]
        ) and not ignore_default:
            cols = from_default + cols

        try:
            return reduce(lambda dct, k: dct[k], cols, d)
        except (KeyError, TypeError):
            # Value should be handled by attrs:
            # Can't return `None` as that is a possible value
            return Skipped()

    def attr_serialization_with_class(cls):
        def from_dict(d, ignore_default=False):
            # loop through all of the fields in the attrs object
            # and find the value from the passed in dictionary

            unnested_dict = {}

            for field in attr.fields(cls):
                val = get_field_value(field, d, ignore_default)
                if not isinstance(val, Skipped):
                    unnested_dict[field.name] = val

            return cls(**unnested_dict)

        cls.from_dict = staticmethod(from_dict)

        return cls

    if cls:
        return attr_serialization_with_class(cls)
    else:
        return attr_serialization_with_class
