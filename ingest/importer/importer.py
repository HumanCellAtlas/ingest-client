class Importer:

    def __init__(self, spec):
        pass

    def do_import(self, worksheet):
        return {
            'project_core':
                {
                    'project_shortname': worksheet['A4'].value,
                    'project_title': worksheet['B4'].value
                }
        }
