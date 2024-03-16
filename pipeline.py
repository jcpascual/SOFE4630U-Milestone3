# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from beam_nuggets.io import relational_db

source_config = relational_db.SourceConfiguration(
    drivername='mysql+pymysql',
    host='34.95.5.203',
    port=3306,
    username='usr',
    password='sofe4630u',
    database='Readings',
)

table_config = relational_db.TableConfiguration(
    name='smartMeter',
    create_if_missing=True
)

class ConvertDoFn(beam.DoFn):

  def process(self, element):
    element['pressure'] = element['pressure'] / 6.895
    element['temperature'] = (element['temperature'] * 1.8) + 32

    return [element]

            
def run(argv=None):
  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--input', dest='input', required=True,
                      help='Input file to process.')
  parser.add_argument('--output', dest='output', required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True;
  
  with beam.Pipeline(options=pipeline_options) as p:
    images= (p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input)
        | "toDict" >> beam.Map(lambda x: json.loads(x)));

    # Filter out all messages with None pressure or temperature.    
    none_filter = images | beam.Filter(lambda x: not x['pressure'] == None and not x['temperature'] == None)

    # Convert the units of pressure and temperature.
    unit_convert = none_filter | 'Convert' >> beam.ParDo(ConvertDoFn())
    
    (unit_convert | 'to byte' >> beam.Map(lambda x: json.dumps(x).encode('utf8'))
        |   'to Pub/sub' >> beam.io.WriteToPubSub(topic=known_args.output));
    
    (unit_convert | 'to DB' >> relational_db.Write(
        source_config=source_config,
        table_config=table_config
    ))
        
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
