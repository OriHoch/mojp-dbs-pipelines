from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import datetime, logging


class Processor(BaseProcessor):

    @staticmethod
    def get_time_gap(start_time):
        current = datetime.datetime.now()
        minutes = (current - start_time).total_seconds() / 60
        return minutes

    def _filter_resource(self, resource_descriptor, resource_data):
        self._stats["delay_limit yielded rows"] = 0
        start_time = datetime.datetime.now()
        delay_limit = float(self._parameters.get("stop-after-minutes"))
        reached_limit = False
        for row in resource_data:
            if not reached_limit:
                time_gap = self.get_time_gap(start_time)
                if time_gap > delay_limit:
                    self._warn_once("ran for {} minutes, reached delay limit".format(time_gap))
                    break
                else:
                    yield row
                    self._stats["delay_limit yielded rows"] += 1


if __name__ == '__main__':
    Processor.main()
