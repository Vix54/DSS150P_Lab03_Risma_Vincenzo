class PipelineError(Exception):
    def __init__(self, stage, message):
        super().__init__(f'[{stage}] {message}')
        self.stage = stage
