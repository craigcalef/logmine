#from .processor import Processor
from logmine.continous_processor import ContinuousProcessor
from .output import Output
from .debug import log


class LogMineContinous():
    def __init__(self, processor_config, cluster_config, output_options):
        log(
            "LogMine: init with config:",
            processor_config,
            cluster_config, output_options
        )
        self.processor = ContinuousProcessor(processor_config, cluster_config)
        self.output = Output(output_options)
 
    def process(self, data):
        self.processor.process(data)

    def end(self):
        clusters = self.processor.complete()
        self.output.out(cluster)
 

#    def run(self, files):
#        log("LogMine: run with files:", files)
#        clusters = self.processor.process(files)
#        log("LogMine: output cluster:", clusters)
#        self.output.out(clusters)
