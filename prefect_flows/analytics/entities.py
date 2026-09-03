"""
Named-entity extraction — Stage A, reads doc.ents directly

doc.ents is already populated by the same get_doc(text) call used for lexical.py/syntactic.py (en_core_web_sm's default pipeline includes ner — see nlp_pipeline.py)
This module does no parsing of its own, it only reads and groups what's already on the Doc.
"""
from collections import Counter, defaultdict
from typing import Dict

# Groups spaCy's default NER labels into categories more useful for political-speech analysis than the raw label set 
# NORP (nationalities / religious / political groups — "Republicans", "Christians", "Americans") gets its own bucket rather than folding into organizations, because collapsing it into "organizations" would bury it
ENTITY_GROUPS: Dict[str, str] = {
    "PERSON": "persons",
    "ORG": "organizations",
    "NORP": "groups",
    "GPE": "locations",
    "LOC": "locations",
    "FAC": "locations",
    "DATE": "dates",
    "TIME": "dates",
}
DEFAULT_GROUP = "other"  # EVENT, WORK_OF_ART, LAW, LANGUAGE, PRODUCT, PERCENT, MONEY, QUANTITY, ORDINAL, CARDINAL, ...


def extract_entities(doc, top_n: int = 25) -> Dict:
    """
    Groups doc.ents into persons/organizations/groups/locations/dates/other
    Mentions are deduplicated by lowercased text (e.g. "Biden" and "biden" merge into one count), keeping the most frequent original casing as the display text. 
    Each group is capped to its top_n most frequent mentions. Also returns raw per-label counts (by_label) for completeness/debugging
    """
    grouped = defaultdict(Counter)          # group -> Counter(lower_text -> count)
    casing = defaultdict(Counter)           # (group, lower_text) -> Counter(original_text -> count)
    by_label = Counter()                    # raw spaCy label -> count

    for ent in doc.ents:
        text = ent.text.strip()
        if not text:
            continue
        group = ENTITY_GROUPS.get(ent.label_, DEFAULT_GROUP)
        lower = text.lower()
        grouped[group][lower] += 1
        casing[(group, lower)][text] += 1
        by_label[ent.label_] += 1

    result = {}
    for group, counter in grouped.items():
        entries = []
        for lower_text, count in counter.most_common(top_n):
            representative_text = casing[(group, lower_text)].most_common(1)[0][0]
            entries.append({"text": representative_text, "count": count})
        result[group] = entries

    result["by_label"] = dict(by_label)
    result["total_entities"] = sum(by_label.values())
    return result