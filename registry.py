#-*- coding: utf-8 -*-


class Registry:

    def __init__(self, registry_df, _type):
        self.registry = registry_df
        self.type = _type
        self.cols = set(self.registry.columns)

    def _check_cols(self, *valid_cols):
        return valid_cols & self.cols != valid_cols

    def _no_duplicates(self, *verif_cols):
        return self.registry[verif_cols].duplicated(keep=False).empty

    
