from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import logging, datetime, importlib


class Processor(BaseProcessor):

    def _filter_update_value(self, v):
        if v.startswith("(") and v.endswith(")"):
            cmdparts = v[1:-1].split(":")
            cmdmodule = cmdparts[0]
            cmdfunc = cmdparts[1]
            cmdargs = cmdparts[2] if len(cmdparts) > 2 else None
            func = importlib.import_module(cmdmodule)
            for part in cmdfunc.split("."):
                func = getattr(func, part)
            if cmdargs == "args":
                v = func(v)
            else:
                v = func()
        return v

    def _filter_row(self, row):
        table = self.db_meta.tables[self._parameters["table"]]
        table.update()\
            .where(getattr(table.c, self._parameters["id-column"])==row[self._parameters["id-field"]])\
            .values(**{k: self._filter_update_value(v) for k, v in self._parameters["fields"].items()})\
            .execute()
        self.db_commit()
        return row

if __name__ == '__main__':
    Processor.main()
