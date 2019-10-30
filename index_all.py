import os

from github import Github

from index_selfdescribe import index_selfdescribe

if __name__ == '__main__':
    repo_name = 'signalfx/signalfx-agent'
    token = os.environ.get('GITHUB_PERSONAL_ACCESS_TOKEN')
    # tag/sha/se
    g = Github(login_or_token=token)
    r = g.get_repo(repo_name)
    release_tags = [r.tag_name for r in r.get_releases()]
    sha_list = [branch.commit.sha for branch in r.get_branches() if branch.name == 'master'] + [tag.commit.sha for tag in r.get_tags() if tag.name in release_tags]
    index_selfdescribe(repo_name, token, sha_list)
