class DataSource:
    # attributes
    data_dir = ''
    data_filename = ''

    def __init__(self, data_dir, data_filename):
        self.data_dir = data_dir
        self.data_filename = data_filename

    def get_path(self):
        return self.data_dir + self.data_filename
