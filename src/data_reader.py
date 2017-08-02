import json
from dateutil.parser import *
import pickle


class DataReader:
    def __init__(self):

        self.iter_user = 1
        self.iter_repo = 1
        self.iter_type = 1
        self.user_num = dict()
        self.repo_num = dict()
        self.type_num = dict()
        self.actor_dict = dict()

    def read(self, file):

        with open(file) as f:
            lines = f.readlines()
            for line in lines:
                entry = json.loads(line)
                self.add_entry(entry)
        for arrs in self.actor_dict.values():
            arrs.sort(key=lambda x: x['created_at'], reverse=False)
        return self.actor_dict

    def save(self, file):
        with open(file, 'wb') as f:
            pickle.dump(dr, f, pickle.HIGHEST_PROTOCOL)

    def add_entry(self, entry):
        actor = entry['actor']
        repo = entry['repo']
        atype = entry['type']
        created_at = parse(entry['created_at'])

        if not (actor and self.user_num.get(actor, False)):
            self.user_num[actor] = self.iter_user
            self.iter_user += 1
        if not (repo and self.repo_num.get(repo, False)):
            self.repo_num[repo] = self.iter_repo
            self.iter_repo += 1
        if not (atype and self.type_num.get(atype, False)):
            self.type_num[atype] = self.iter_type
            self.iter_type += 1

        self.actor_dict.setdefault(self.user_num[actor], list())
        self.actor_dict[self.user_num[actor]].append({'created_at': created_at,
                                                      'repo_id': self.repo_num[repo],
                                                      'type': self.type_num[atype]})