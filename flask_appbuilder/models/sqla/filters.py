import logging
import datetime
from dateutil import parser
from flask_babel import lazy_gettext
from ..filters import BaseFilter, FilterRelation, BaseFilterConverter, FilterGroup
from sqlalchemy import or_

log = logging.getLogger(__name__)


__all__ = ['SQLAFilterConverter', 'FilterEqual', 'FilterNotStartsWith', 'FilterStartsWith', 'FilterContains',
           'FilterNotEqual', 'FilterEndsWith', 'FilterEqualFunction', 'FilterGreater', 'FilterNotEndsWith',
           'FilterRelationManyToManyEqual', 'FilterRelationOneToManyEqual', 'FilterRelationOneToManyNotEqual',
           'FilterSmaller']

def get_field_setup_query(query, model, column_name, joins):
    """
        Help function for SQLA filters, checks for dot notation on column names.
        If it exists, will join the query with the model from the first part of the field name.

        example:
            Contact.created_by: if created_by is a User model, it will be joined to the query.
    """
    if not hasattr(model, column_name):
       # it's an inner obj attr
        rel_model = getattr(model, column_name.split('.')[0]).mapper.class_
        if rel_model not in joins:
            query = query.join(rel_model)
            joins.add(rel_model)
        #return query, getattr(rel_model, column_name.split('.')[1])
        return get_field_setup_query(query, rel_model, column_name[column_name.index('.')+1:], joins)
    else:
        return query, getattr(model, column_name)


def set_value_to_type(datamodel, column_name, value):
    if datamodel.is_integer(column_name):
        try:
            return int(value)
        except Exception as e:
            return None
    elif datamodel.is_float(column_name):
        try:
            return float(value)
        except Exception as e:
            return None
    elif datamodel.is_boolean(column_name):
            if value == 'y':
                return True
    elif datamodel.is_date(column_name) and not isinstance(value, datetime.date):
        try:
            return parser.parse(value).date()
        except Exception as e:
            return None
    elif datamodel.is_datetime(column_name) and not isinstance(value, datetime.datetime):
        try:
            return parser.parse(value)
        except Exception as e:
            return None
    return value


class FilterOr(FilterGroup):
    def apply(self, query, joins=set()):
        if self.is_applied is None or self.is_applied():
            if len(self.sub_filters) > 1:
                member_list = []
                for item in self.sub_filters:
                    column_name, filter_class, value = item
                    flt = filter_class(column_name, self.filter.datamodel)
                    query, member = flt.apply(query, value, True, joins)
                    member_list.append(member)

                query = query.filter(or_(*member_list))
            elif len(self.sub_filters) == 1:
                column_name, filter_class, value = self.sub_filters[0]
                flt = filter_class(column_name, self.filter.datamodel)
                query = flt.apply(query, value, joins=joins)

        return query


class FilterStartsWith(BaseFilter):
    name = lazy_gettext('Starts with')

    def apply(self, query, value, for_or=False, joins=set()):
        if not for_or:
            query, field = get_field_setup_query(query, self.model, self.column_name, joins)
            return query.filter(field.like(value + '%'))
        else:
            query, field = get_field_setup_query(query, self.model, self.column_name, joins)
            return query, field.like(value + '%')



class FilterNotStartsWith(BaseFilter):
    name = lazy_gettext('Not Starts with')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        return query.filter(~field.like(value + '%'))


class FilterEndsWith(BaseFilter):
    name = lazy_gettext('Ends with')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        return query.filter(field.like('%' + value))


class FilterNotEndsWith(BaseFilter):
    name = lazy_gettext('Not Ends with')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        return query.filter(~field.like('%' + value))


class FilterContains(BaseFilter):
    name = lazy_gettext('Contains')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        return query.filter(field.like('%' + value + '%'))


class FilterNotContains(BaseFilter):
    name = lazy_gettext('Not Contains')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        return query.filter(~field.like('%' + value + '%'))


class FilterEqual(BaseFilter):
    name = lazy_gettext('Equal to')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        value = set_value_to_type(self.datamodel, self.column_name, value)
        return query.filter(field == value)


class FilterNotEqual(BaseFilter):
    name = lazy_gettext('Not Equal to')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        value = set_value_to_type(self.datamodel, self.column_name, value)
        return query.filter(field != value)


class FilterGreater(BaseFilter):
    name = lazy_gettext('Greater than')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        value = set_value_to_type(self.datamodel, self.column_name, value)
        return query.filter(field > value)


class FilterSmaller(BaseFilter):
    name = lazy_gettext('Smaller than')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        value = set_value_to_type(self.datamodel, self.column_name, value)
        return query.filter(field < value)


class FilterRelationOneToManyEqual(FilterRelation):
    name = lazy_gettext('Relation')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        rel_obj = self.datamodel.get_related_obj(self.column_name, value)
        return query.filter(field == rel_obj)


class FilterRelationOneToManyNotEqual(FilterRelation):
    name = lazy_gettext('No Relation')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        rel_obj = self.datamodel.get_related_obj(self.column_name, value)
        return query.filter(field != rel_obj)


class FilterRelationManyToManyEqual(FilterRelation):
    name = lazy_gettext('Relation as Many')

    def apply(self, query, value, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        rel_obj = self.datamodel.get_related_obj(self.column_name, value)
        return query.filter(field.contains(rel_obj))


class FilterEqualFunction(BaseFilter):
    name = "Filter view with a function"

    def apply(self, query, func, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        return query.filter(field == func())


class FilterInFunction(BaseFilter):
    name = "Filter view where field is in a list returned by a function"

    def apply(self, query, func, for_or=False, joins=set()):
        query, field = get_field_setup_query(query, self.model, self.column_name, joins)
        return query.filter(field.in_(func()))


class SQLAFilterConverter(BaseFilterConverter):
    """
        Class for converting columns into a supported list of filters
        specific for SQLAlchemy.

    """
    conversion_table = (('is_relation_many_to_one', [FilterRelationOneToManyEqual,
                        FilterRelationOneToManyNotEqual]),
                        ('is_relation_one_to_one', [FilterRelationOneToManyEqual,
                        FilterRelationOneToManyNotEqual]),
                        ('is_relation_many_to_many', [FilterRelationManyToManyEqual]),
                        ('is_relation_one_to_many', [FilterRelationManyToManyEqual]),
                        ('is_enum', [FilterEqual,
                                     FilterNotEqual]),
                        ('is_text', [FilterStartsWith,
                                     FilterEndsWith,
                                     FilterContains,
                                     FilterEqual,
                                     FilterNotStartsWith,
                                     FilterNotEndsWith,
                                     FilterNotContains,
                                     FilterNotEqual]),
                        ('is_binary', [FilterStartsWith,
                                       FilterEndsWith,
                                       FilterContains,
                                       FilterEqual,
                                       FilterNotStartsWith,
                                       FilterNotEndsWith,
                                       FilterNotContains,
                                       FilterNotEqual]),
                        ('is_string', [FilterStartsWith,
                                       FilterEndsWith,
                                       FilterContains,
                                       FilterEqual,
                                       FilterNotStartsWith,
                                       FilterNotEndsWith,
                                       FilterNotContains,
                                       FilterNotEqual]),
                        ('is_integer', [FilterEqual,
                                        FilterGreater,
                                        FilterSmaller,
                                        FilterNotEqual]),
                        ('is_float', [FilterEqual,
                                      FilterGreater,
                                      FilterSmaller,
                                      FilterNotEqual]),
                        ('is_numeric', [FilterEqual,
                                      FilterGreater,
                                      FilterSmaller,
                                      FilterNotEqual]),
                        ('is_date', [FilterEqual,
                                     FilterGreater,
                                     FilterSmaller,
                                     FilterNotEqual]),
                        ('is_boolean', [FilterEqual,
                                        FilterNotEqual]),
                        ('is_datetime', [FilterEqual,
                                         FilterGreater,
                                         FilterSmaller,
                                         FilterNotEqual]),
    )
