from main.sources.agent_knowledge.skill_frontmatter import parse_skill_markdown


def test_parse_valid_skill_frontmatter():
    parsed = parse_skill_markdown('''---
name: code-review
description: Review code changes. Use for pull requests.
license: MIT
compatibility: Requires git
allowed-tools: Bash(git:*) Read
metadata:
  owner: platform
  version: "1.0"
---
# Instructions
Review carefully.
''')

    assert parsed['warnings'] == []
    assert parsed['frontmatter']['name'] == 'code-review'
    assert parsed['frontmatter']['description'] == 'Review code changes. Use for pull requests.'
    assert parsed['frontmatter']['metadata']['owner'] == 'platform'
    assert parsed['frontmatter']['metadata']['version'] == '1.0'
    assert '# Instructions' in parsed['body']


def test_parse_missing_frontmatter_warns_for_required_fields():
    parsed = parse_skill_markdown('# No frontmatter')

    assert parsed['frontmatter'] == {}
    assert 'Missing required skill frontmatter field: name' in parsed['warnings']
    assert 'Missing required skill frontmatter field: description' in parsed['warnings']


def test_parse_missing_required_field():
    parsed = parse_skill_markdown('''---
name: review
---
Body
''')

    assert parsed['frontmatter']['name'] == 'review'
    assert parsed['warnings'] == ['Missing required skill frontmatter field: description']
