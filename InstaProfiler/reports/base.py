from typing import Dict
from typing import List, Optional
from typing import Tuple


class AsTableRow(object):
    @classmethod
    def get_export_fields(cls) -> List[str]:
        raise NotImplementedError()


class AsGroupedTableRow(AsTableRow):
    @classmethod
    def get_group_by_fields(cls) -> List[str]:
        raise NotImplementedError()

    @classmethod
    def get_cleaned_export_fields(cls):
        clean_export_fields = cls.get_export_fields().copy()
        [clean_export_fields.remove(field) for field in cls.get_group_by_fields()]
        return clean_export_fields

    @classmethod
    def get_ordered_export_fields(cls):
        """Just makes sure that the group_by field is the first in the export order"""
        clean_export_fields = cls.get_cleaned_export_fields()
        return [cls.get_group_by_fields()] + clean_export_fields


class HTMLTableConverter(object):

    @staticmethod
    def opt_add_header(headers: Optional[List[str]], rows_lst: list, item_fields_amount: int):
        if headers is not None:
            assert len(headers) == item_fields_amount
            headers = ''.join("<th>{0}</th>".format(x) for x in headers)
            rows_lst.append("<tr>{0}</tr>".format(headers))

    @classmethod
    def to_table(cls, items: List[AsTableRow], headers: Optional[List[str]] = None) -> str:
        rows = []
        cls.opt_add_header(headers, rows, len(items[0].get_export_fields()))
        for item in items:
            data = [getattr(item, field) for field in item.get_export_fields()]
            cells = ''.join("<td>{0}</td>".format(x) for x in data)
            rows.append("<tr>{0}</tr>".format(cells))
        return """<table rules="all" border="2"><tbody>\n{0}\n</tbody></table>""".format('\n'.join(rows))

    @classmethod
    def to_grouped_table(cls, items: List[AsGroupedTableRow], headers: Optional[List[str]] = None) -> str:
        rows = []
        cls.opt_add_header(headers, rows, len(items[0].get_export_fields()))
        groups = {}  # type: Dict[Tuple, List[AsGroupedTableRow]]
        group_by_fields = items[0].get_group_by_fields()
        for item in items:
            key = tuple((getattr(item, field) for field in group_by_fields))
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        sorted_groups = sorted(groups.items(), key=lambda tuple: len(tuple[1]),
                               reverse=True)  # type: List[Tuple[Tuple, List[AsGroupedTableRow]]]

        for group, items in sorted_groups:
            group_rows = []
            first_item = items[0]
            first_spanning_cells = ["""<td rowspan="{}">{}</td>""".format(len(items), getattr(first_item, field)) for
                                    field in
                                    first_item.get_group_by_fields()]
            first_remaining_cells = ["""<td>{}</td>""".format(getattr(first_item, field)) for field in
                                     first_item.get_cleaned_export_fields()]
            first_row = ''.join(first_spanning_cells + first_remaining_cells)
            group_rows.append("<tr>{}</tr>".format(first_row))

            if len(items) > 1:
                for item in items[1:]:
                    item_data = [getattr(item, field) for field in first_item.get_cleaned_export_fields()]
                    item_cells = ''.join("<td>{}</td>".format(x) for x in item_data)
                    group_rows.append("<tr>{}</tr>".format(item_cells))

            rows.append("<tbody>{}</tbody>".format('\n'.join(group_rows)))
        return """<table rules="all" border="2">\n{0}\n</table>""".format('\n'.join(rows))
