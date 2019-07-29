import pathlib
import itertools
import json
import bonobo

class PipelineBase:
	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = {
			'trace_counter': itertools.count(),
			f'fs.data.{self.project_name}': bonobo.open_fs(self.input_path)
		}

		common_path = pathlib.Path(self.pipeline_common_service_files_path)
		for file in common_path.rglob('*.json'):
			with open(file, 'r') as f:
				services[file.stem] = json.load(f)

		proj_path = pathlib.Path(self.pipeline_project_service_files_path)
		for file in proj_path.rglob('*.json'):
			with open(file, 'r') as f:
				if file.stem in services:
					print(f'*** Project is overloading a shared service JSON file: {file.stem}')
				services[file.stem] = json.load(f)

		return services
