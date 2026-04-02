# ---------------------------------------------------------------------------
# Problem: Spell Checker Using a Bloom Filter
# ---------------------------------------------------------------------------
# Build a fast spell checker backed by a Bloom Filter.
#
# 1. Load a dictionary of valid English words into the Bloom Filter.
# 2. Given a list of words, classify each as:
#      - "definitely misspelled" (Bloom filter says NOT in set — zero false
#        negatives, so this is certain)
#      - "probably correct"      (Bloom filter says MIGHT be in set — could
#        be a false positive)
# 3. Demonstrate how false positives manifest: some misspelled words slip
#    past the filter and are incorrectly marked "probably correct".
#
# Complexity (per query):
#   Time:  O(k)  where k = number of hash functions
#   Space: O(m)  for the entire filter (m = bit-array size)
# ---------------------------------------------------------------------------

from __future__ import annotations

import os
import sys

# Make the DataStructures package importable.
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(_project_root, "DataStructures"))

from BloomFilter import BloomFilter


# -- Dictionary ---------------------------------------------------------------
# A small curated word list for demonstration purposes.
DICTIONARY: list[str] = [
    "abandon", "ability", "able", "about", "above", "absent", "absorb",
    "abstract", "absurd", "abuse", "accept", "access", "accident", "account",
    "accuse", "achieve", "acid", "acoustic", "acquire", "across", "act",
    "action", "actor", "actual", "adapt", "add", "addict", "address",
    "adjust", "admit", "adult", "advance", "advice", "aerobic", "afraid",
    "again", "age", "agent", "agree", "ahead", "aim", "air", "airport",
    "aisle", "alarm", "album", "alert", "alien", "all", "allow",
    "almost", "alone", "alpha", "already", "also", "alter", "always",
    "amateur", "amazing", "among", "amount", "amused", "anchor", "ancient",
    "anger", "angle", "angry", "animal", "announce", "annual", "another",
    "answer", "antenna", "antique", "anxiety", "any", "apart", "apology",
    "appear", "apple", "approve", "april", "arena", "argue", "army",
    "arrive", "arrow", "art", "artist", "artwork", "ask", "aspect",
    "assault", "asset", "assist", "assume", "asthma", "athlete", "atom",
    "attack", "attend", "auction", "august", "aunt", "author", "auto",
    "avocado", "avoid", "awake", "aware", "awesome", "awful", "awkward",
    "baby", "bachelor", "bacon", "badge", "bag", "balance", "ball",
    "banana", "banner", "bar", "barely", "bargain", "barrel", "base",
    "basic", "basket", "battle", "beach", "bean", "beauty", "because",
    "become", "beef", "before", "begin", "behind", "believe", "below",
    "belt", "bench", "benefit", "best", "betray", "better", "between",
    "beyond", "bicycle", "bird", "birth", "bitter", "black", "blade",
    "blame", "blank", "blast", "bleak", "bless", "blind", "blood",
    "blossom", "blue", "blur", "blush", "board", "boat", "body",
    "boil", "bomb", "bone", "bonus", "book", "boost", "border",
    "boring", "boss", "bottom", "bounce", "box", "boy", "bracket",
    "brain", "brand", "brass", "brave", "bread", "breeze", "brick",
    "bridge", "brief", "bright", "bring", "brisk", "broken", "bronze",
    "brother", "brown", "brush", "bubble", "buddy", "budget", "buffalo",
    "build", "bulb", "bulk", "bullet", "bundle", "burger", "burst",
    "bus", "business", "busy", "butter", "buyer", "buzz",
    "cabbage", "cabin", "cable", "cactus", "cage", "cake", "call",
    "calm", "camera", "camp", "can", "canal", "cancel", "candy",
    "capable", "capital", "captain", "car", "carbon", "card", "cargo",
    "carpet", "carry", "cart", "case", "cash", "casino", "castle",
    "catalog", "catch", "category", "cattle", "caught", "cause", "caution",
    "cave", "ceiling", "celery", "cement", "census", "century", "cereal",
    "certain", "chair", "chalk", "champion", "change", "chaos", "chapter",
    "charge", "charity", "charm", "chart", "chase", "cheap", "check",
    "cheese", "cherry", "chicken", "chief", "child", "chimney", "choice",
    "chunk", "church", "circle", "citizen", "city", "civil", "claim",
    "clap", "class", "clean", "clerk", "clever", "cliff", "climb",
    "clock", "close", "cloth", "cloud", "clown", "club", "cluster",
    "coach", "coast", "code", "coffee", "coin", "cold", "collect",
    "color", "column", "combine", "come", "comfort", "comic", "common",
    "company", "concert", "conduct", "confirm", "congress", "connect",
    "consider", "control", "convince", "cook", "cool", "copper", "copy",
    "coral", "core", "corn", "correct", "cost", "cotton", "couch",
    "country", "couple", "course", "cousin", "cover", "craft", "crash",
    "crater", "crazy", "cream", "credit", "creek", "crew", "cricket",
    "crime", "crisp", "critic", "crop", "cross", "crowd", "crucial",
    "cruel", "cruise", "crumble", "crush", "crystal", "cube", "culture",
    "cup", "curtain", "curve", "cushion", "custom", "cute", "cycle",
    "dad", "damage", "dance", "danger", "dark", "dash", "daughter",
    "dawn", "day", "deal", "debate", "december", "decide", "decline",
    "decorate", "decrease", "deer", "defense", "define", "degree", "delay",
    "deliver", "demand", "dentist", "deny", "depart", "depend", "deposit",
    "depth", "derive", "describe", "desert", "design", "desk", "detail",
    "detect", "develop", "device", "devote", "diagram", "dial", "diamond",
    "diary", "diesel", "diet", "differ", "digital", "dignity", "dilemma",
    "dinner", "dinosaur", "direct", "dirt", "disagree", "discover",
    "disease", "dish", "dismiss", "display", "distance", "divert", "divide",
    "dizzy", "doctor", "document", "dog", "domain", "donate", "donkey",
    "door", "dose", "double", "dove", "draft", "dragon", "drama",
    "drastic", "draw", "dream", "dress", "drift", "drill", "drink",
    "drip", "drive", "drop", "drum", "dry", "duck", "dumb",
    "dune", "during", "dust", "dutch", "duty", "dwarf", "dynamic",
    "eager", "eagle", "earth", "easily", "east", "easy", "echo",
    "ecology", "economy", "edge", "edit", "educate", "effort", "eight",
    "either", "elbow", "elder", "electric", "elegant", "element", "elephant",
    "elevator", "elite", "else", "embark", "embody", "embrace", "emerge",
    "emotion", "employ", "empower", "empty", "enable", "enact", "end",
    "endless", "endorse", "enemy", "energy", "enforce", "engage", "engine",
    "enhance", "enjoy", "enlist", "enough", "enrich", "enroll", "ensure",
    "enter", "entire", "entry", "envelope", "episode", "equal", "equip",
    "era", "erode", "erosion", "error", "erupt", "escape", "essay",
    "essence", "estate", "eternal", "evidence", "evil", "evolve", "exact",
    "example", "excess", "exchange", "excite", "exclude", "excuse",
    "execute", "exercise", "exhaust", "exhibit", "exile", "exist", "exit",
    "exotic", "expand", "expect", "expire", "explain", "expose", "express",
    "extend", "extra", "eye", "fabric", "face", "faculty", "fade",
    "faint", "faith", "fall", "false", "fame", "family", "famous",
    "fan", "fancy", "fantasy", "farm", "fashion", "fat", "fatal",
    "father", "fatigue", "fault", "favorite", "feature", "february",
    "federal", "fee", "feed", "feel", "female", "fence", "festival",
    "fetch", "fever", "few", "fiber", "fiction", "field", "figure",
    "file", "film", "filter", "final", "find", "finger", "finish",
    "fire", "firm", "fiscal", "fish", "fit", "fitness", "fix",
    "flag", "flame", "flash", "flat", "flavor", "flee", "flight",
    "flip", "float", "flock", "floor", "flower", "fluid", "flush",
    "fly", "foam", "focus", "fog", "foil", "fold", "follow",
    "food", "foot", "force", "forest", "forget", "fork", "fortune",
    "forum", "forward", "fossil", "foster", "found", "fox", "fragile",
    "frame", "frequent", "fresh", "friend", "fringe", "frog", "front",
    "frozen", "fruit", "fuel", "fun", "funny", "furnace", "fury",
    "future", "galaxy", "game", "gap", "garage", "garden", "garlic",
    "gas", "gasp", "gate", "gather", "gauge", "gaze", "general",
    "genius", "gentle", "genuine", "gesture", "ghost", "giant", "gift",
    "giggle", "giraffe", "girl", "give", "glad", "glance", "glass",
    "globe", "gloom", "glory", "glove", "glow", "glue", "goat",
    "goddess", "gold", "good", "goose", "gospel", "gossip", "govern",
    "grace", "grain", "grant", "grape", "grass", "gravity", "great",
    "green", "grid", "grief", "grit", "grocery", "group", "grow",
    "grunt", "guard", "guess", "guide", "guilt", "guitar", "gun",
    "gym", "habit", "hair", "half", "hammer", "hamster", "hand",
    "happy", "harbor", "hard", "harsh", "harvest", "hat", "have",
    "hawk", "hazard", "head", "health", "heart", "heavy", "hedgehog",
    "height", "hello", "helmet", "help", "hen", "hero", "hidden",
    "high", "hill", "hint", "hip", "history", "hobby", "hockey",
    "hold", "hole", "holiday", "home", "honey", "hood", "hope",
    "horn", "horror", "horse", "hospital", "host", "hotel", "hour",
    "hover", "hub", "huge", "human", "humble", "humor", "hundred",
    "hungry", "hunt", "hurdle", "hurry", "hurt", "husband", "hybrid",
    "ice", "icon", "idea", "identify", "idle", "ignore", "image",
    "imitate", "immense", "immune", "impact", "impose", "improve",
    "impulse", "inch", "include", "income", "increase", "index",
    "indicate", "indoor", "industry", "infant", "inflict", "inform",
    "initial", "inject", "inner", "innocent", "input", "inquiry",
    "insane", "insect", "inside", "inspire", "install", "intact",
    "interest", "into", "invest", "invite", "iron", "island", "isolate",
    "issue", "item", "ivory", "jacket", "jaguar", "jar", "jazz",
    "jealous", "jeans", "jelly", "jewel", "job", "join", "joke",
    "journey", "joy", "judge", "juice", "jump", "jungle", "junior",
    "junk", "just", "kangaroo", "keen", "keep", "kernel", "key",
    "kick", "kid", "kidney", "kind", "kingdom", "kiss", "kitchen",
    "kite", "kitten", "knee", "knife", "knock", "know", "label",
    "labor", "ladder", "lake", "lamp", "language", "laptop", "large",
    "later", "latin", "laugh", "laundry", "lava", "law", "lawn",
    "lawsuit", "layer", "lazy", "leader", "leaf", "learn", "leave",
    "lecture", "left", "legal", "legend", "leisure", "lemon", "length",
    "lens", "leopard", "lesson", "letter", "level", "liberty", "library",
    "license", "life", "lift", "light", "like", "limb", "limit",
    "link", "lion", "liquid", "list", "little", "live", "lizard",
    "load", "loan", "lobster", "local", "lock", "logic", "lonely",
    "long", "loop", "lottery", "loud", "love", "loyal", "lucky",
    "lumber", "lunch", "luxury",
    "machine", "magic", "magnet", "maid", "main", "major", "make",
    "mammal", "man", "manage", "mandate", "mango", "mansion", "manual",
    "maple", "marble", "march", "margin", "marine", "market", "mass",
    "master", "match", "material", "math", "matrix", "matter", "maximum",
    "maze", "meadow", "mean", "measure", "meat", "mechanic", "media",
    "melody", "member", "memory", "mention", "menu", "mercy", "merge",
    "merit", "method", "middle", "milk", "million", "mimic", "mind",
    "minimum", "minor", "minute", "miracle", "mirror", "misery", "miss",
    "mistake", "mix", "model", "modify", "moment", "monitor", "monkey",
    "monster", "month", "moon", "moral", "morning", "mosquito", "mother",
    "motion", "motor", "mountain", "mouse", "move", "movie", "much",
    "multiply", "muscle", "museum", "music", "must", "mutual", "myself",
    "mystery", "myth", "naive", "name", "napkin", "narrow", "nation",
    "nature", "near", "neck", "need", "negative", "neglect", "neither",
    "nephew", "nerve", "nest", "net", "network", "neutral", "never",
    "news", "next", "nice", "night", "noble", "noise", "normal",
    "north", "notable", "nothing", "notice", "novel", "now", "nuclear",
    "number", "nurse", "nut", "oak", "obey", "object", "oblige",
    "obscure", "observe", "obtain", "obvious", "occur", "ocean", "october",
    "odor", "off", "offer", "office", "often", "oil", "okay",
    "old", "olive", "olympic", "omit", "once", "one", "onion",
    "online", "only", "open", "opera", "opinion", "oppose", "option",
    "orange", "orbit", "orchard", "order", "ordinary", "organ", "orient",
    "original", "orphan", "ostrich", "other", "outdoor", "outer", "output",
    "outside", "oval", "oven", "over", "own", "owner", "oxygen",
    "oyster", "ozone",
    "package", "paddle", "page", "pair", "palace", "palm", "panda",
    "panel", "panic", "panther", "paper", "parade", "parent", "park",
    "parrot", "party", "pass", "patch", "path", "patient", "patrol",
    "pattern", "pause", "pave", "payment", "peace", "peanut", "pear",
    "peasant", "pelican", "pen", "penalty", "pencil", "people", "pepper",
    "perfect", "permit", "person", "pet", "phone", "photo", "phrase",
    "piano", "picnic", "picture", "piece", "pig", "pigeon", "pill",
    "pilot", "pink", "pioneer", "pipe", "pistol", "pitch", "pizza",
    "place", "planet", "plastic", "plate", "play", "please", "pledge",
    "pluck", "plug", "plunge", "poem", "poet", "point", "polar",
    "pole", "police", "pond", "pony", "pool", "popular", "position",
    "possible", "post", "potato", "pottery", "poverty", "powder", "power",
    "practice", "praise", "predict", "prefer", "prepare", "present",
    "pretty", "prevent", "price", "pride", "primary", "print", "priority",
    "prison", "private", "prize", "problem", "process", "produce",
    "profit", "program", "project", "promote", "proof", "property",
    "protect", "proud", "provide", "public", "pudding", "pull", "pulp",
    "pulse", "pumpkin", "punch", "pupil", "puppy", "purchase", "purity",
    "purpose", "purse", "push", "put", "puzzle", "pyramid",
    "quality", "quantum", "quarter", "question", "quick", "quit", "quiz",
    "quote", "rabbit", "raccoon", "race", "rack", "radar", "radio",
    "rail", "rain", "raise", "rally", "ramp", "ranch", "random",
    "range", "rapid", "rare", "rate", "rather", "raven", "raw",
    "razor", "ready", "real", "reason", "rebel", "rebuild", "recall",
    "receive", "recipe", "record", "recycle", "reduce", "reflect",
    "reform", "region", "regret", "regular", "reject", "relax", "release",
    "relief", "rely", "remain", "remember", "remind", "remove", "render",
    "renew", "rent", "reopen", "repair", "repeat", "replace", "report",
    "require", "rescue", "resemble", "resist", "resource", "response",
    "result", "retire", "retreat", "return", "reunion", "reveal", "review",
    "reward", "rhythm", "rib", "ribbon", "rice", "rich", "ride",
    "rifle", "right", "rigid", "ring", "riot", "ripple", "risk",
    "ritual", "rival", "river", "road", "roast", "robot", "robust",
    "rocket", "romance", "roof", "rookie", "room", "rose", "rotate",
    "rough", "round", "route", "royal", "rubber", "rude", "rug",
    "rule", "run", "runway", "rural", "sad", "saddle", "sadness",
    "safe", "sail", "salad", "salmon", "salon", "salt", "salute",
    "same", "sample", "sand", "satisfy", "satoshi", "sauce", "sausage",
    "save", "say", "scale", "scan", "scatter", "scene", "scheme",
    "school", "science", "scissors", "scorpion", "scout", "scrap",
    "screen", "script", "scrub", "sea", "search", "season", "seat",
    "second", "secret", "section", "security", "seed", "seek", "select",
    "sell", "senior", "sense", "sentence", "series", "service", "session",
    "settle", "setup", "seven", "shadow", "shaft", "shallow", "share",
    "shed", "shell", "sheriff", "shield", "shift", "shine", "ship",
    "shiver", "shock", "shoe", "short", "shoulder", "shove", "shrimp",
    "shrug", "shuffle", "shy", "sibling", "sick", "side", "siege",
    "sight", "sign", "silent", "silk", "silly", "silver", "similar",
    "simple", "since", "sing", "siren", "sister", "situate", "six",
    "size", "skate", "sketch", "ski", "skill", "skin", "skirt",
    "skull", "slender", "slice", "slide", "slight", "slim", "slogan",
    "slow", "slush", "small", "smart", "smile", "smoke", "smooth",
    "snake", "snap", "snow", "soap", "soccer", "social", "sock",
    "soda", "soft", "solar", "soldier", "solid", "solution", "solve",
    "someone", "song", "soon", "sorry", "sort", "soul", "sound",
    "soup", "source", "south", "space", "spare", "spatial", "spawn",
    "speak", "special", "speed", "spell", "spend", "sphere", "spider",
    "spike", "spin", "spirit", "split", "sponsor", "spoon", "sport",
    "spot", "spray", "spread", "spring", "spy", "square", "squeeze",
    "stable", "stadium", "staff", "stage", "stairs", "stamp", "stand",
    "start", "state", "stay", "steak", "steel", "stem", "step",
    "stereo", "stick", "still", "sting", "stock", "stomach", "stone",
    "stool", "story", "stove", "strategy", "street", "strike", "strong",
    "struggle", "student", "stuff", "stumble", "style", "subject",
    "submit", "subway", "success", "such", "sudden", "suffer", "sugar",
    "suggest", "suit", "summer", "sun", "sunny", "super", "supply",
    "supreme", "sure", "surface", "surge", "surprise", "surround",
    "survey", "suspect", "sustain", "swallow", "swamp", "swap", "swarm",
    "swear", "sweet", "swim", "swing", "switch", "sword", "symbol",
    "symptom", "syrup", "system",
    "table", "tackle", "tag", "tail", "talent", "talk", "tank",
    "tape", "target", "task", "taste", "tattoo", "taxi", "teach",
    "team", "tell", "ten", "tenant", "tennis", "tent", "term",
    "test", "text", "thank", "that", "theme", "then", "theory",
    "there", "they", "thing", "this", "thought", "three", "thrive",
    "throw", "thumb", "thunder", "ticket", "tide", "tiger", "tilt",
    "timber", "time", "tiny", "tip", "tired", "tissue", "title",
    "toast", "tobacco", "today", "toddler", "together", "toilet",
    "token", "tomato", "tomorrow", "tone", "tongue", "tonight", "tool",
    "tooth", "top", "topic", "topple", "torch", "tornado", "tortoise",
    "total", "tourist", "toward", "tower", "town", "toy", "track",
    "trade", "traffic", "tragic", "train", "transfer", "trap", "trash",
    "travel", "tray", "treat", "tree", "trend", "trial", "tribe",
    "trick", "trigger", "trim", "trip", "trophy", "trouble", "truck",
    "true", "truly", "trumpet", "trust", "truth", "try", "tube",
    "tuna", "tunnel", "turn", "turtle", "twelve", "twenty", "twice",
    "twin", "twist", "two", "type", "typical", "ugly", "umbrella",
    "unable", "unaware", "uncle", "under", "undo", "unfair", "unfold",
    "unhappy", "uniform", "unique", "unit", "universe", "unknown",
    "unlock", "until", "unusual", "unveil", "update", "upgrade", "upon",
    "upper", "urban", "usage", "use", "used", "useful", "useless",
    "usual", "utility", "vacant", "vacuum", "vague", "valid", "valley",
    "valve", "van", "vanish", "vapor", "various", "vast", "vault",
    "vehicle", "velvet", "vendor", "venture", "venue", "verb", "verify",
    "version", "very", "vessel", "veteran", "viable", "victory", "video",
    "view", "village", "vintage", "violin", "virtual", "virus", "visa",
    "visit", "visual", "vital", "vivid", "vocal", "voice", "void",
    "volcano", "volume", "vote", "voyage", "wage", "wagon", "wait",
    "walk", "wall", "walnut", "want", "warfare", "warm", "warrior",
    "wash", "waste", "water", "wave", "way", "wealth", "weapon",
    "wear", "weather", "web", "wedding", "weekend", "welcome", "west",
    "wet", "whale", "what", "wheat", "wheel", "when", "where",
    "whip", "whisper", "wide", "width", "wife", "wild", "will",
    "win", "window", "wine", "wing", "wink", "winner", "winter",
    "wire", "wisdom", "wise", "wish", "witness", "wolf", "woman",
    "wonder", "wood", "wool", "word", "work", "world", "worry",
    "worth", "wrap", "wreck", "wrestle", "wrist", "write", "wrong",
    "yard", "year", "yellow", "you", "young", "youth", "zebra",
    "zero", "zone", "zoo",
]


def build_spell_checker(
    words: list[str],
    expected_items: int | None = None,
    false_positive_rate: float = 0.01,
) -> BloomFilter:
    """Load *words* into a Bloom Filter for spell-checking.

    Args:
        words: Valid dictionary words.
        expected_items: Expected number of items (defaults to len(words)).
        false_positive_rate: Desired false-positive probability.

    Returns:
        A populated BloomFilter.
    """
    if expected_items is None:
        expected_items = max(len(words), 1)
    bf = BloomFilter(
        expected_items=expected_items,
        false_positive_rate=false_positive_rate,
    )
    for w in words:
        bf.add(w.lower())
    return bf


def check_words(
    bf: BloomFilter,
    words: list[str],
    ground_truth: set[str],
) -> dict[str, list[str]]:
    """Check each word against the Bloom Filter and the ground-truth set.

    Returns a dict with four categories:
        true_positive:  word IS in dictionary AND filter says "probably yes"
        true_negative:  word NOT in dictionary AND filter says "definitely no"
        false_positive: word NOT in dictionary BUT filter says "probably yes"
        false_negative: (should never happen with a Bloom Filter)

    Args:
        bf: The Bloom Filter loaded with dictionary words.
        words: Words to check.
        ground_truth: The actual set of valid words.
    """
    results: dict[str, list[str]] = {
        "true_positive": [],
        "true_negative": [],
        "false_positive": [],
        "false_negative": [],
    }
    for w in words:
        w_lower = w.lower()
        in_filter = bf.might_contain(w_lower)
        in_dict = w_lower in ground_truth

        if in_dict and in_filter:
            results["true_positive"].append(w)
        elif not in_dict and not in_filter:
            results["true_negative"].append(w)
        elif not in_dict and in_filter:
            results["false_positive"].append(w)
        else:
            results["false_negative"].append(w)

    return results


if __name__ == "__main__":
    print("=" * 60)
    print("  Spell Checker Using a Bloom Filter")
    print("=" * 60)

    # -- Build the spell checker ------------------------------------------------
    bf = build_spell_checker(DICTIONARY, false_positive_rate=0.05)
    ground_truth = {w.lower() for w in DICTIONARY}

    print(f"\nDictionary size : {len(DICTIONARY)} words")
    print(f"Bloom filter    : {bf}")

    # -- Words to test ----------------------------------------------------------
    correct_words = ["apple", "banana", "guitar", "ocean", "python"]
    misspelled_words = [
        "appple", "bananaa", "guiter", "ocaen", "pyhton",
        "speling", "languge", "funtcion", "recieve", "seperate",
        "definately", "occured", "accomodate", "tommorow", "wierd",
    ]
    # Generate more candidates to surface false positives.
    extra_nonsense = [f"xyzzy{i}" for i in range(200)]

    test_words = correct_words + misspelled_words + extra_nonsense

    # -- Run checks -------------------------------------------------------------
    results = check_words(bf, test_words, ground_truth)

    print("\n--- True Positives (correctly identified as valid) ---")
    print(f"  {results['true_positive']}")

    print("\n--- True Negatives (correctly flagged as misspelled) ---")
    tn = results["true_negative"]
    # Show first 20 for brevity.
    shown = tn[:20]
    print(f"  {shown}{'  ...' if len(tn) > 20 else ''}")
    print(f"  Total: {len(tn)}")

    print("\n--- False Positives (misspelled but filter says 'probably valid') ---")
    print(f"  {results['false_positive']}")
    print(f"  Total: {len(results['false_positive'])}")

    print("\n--- False Negatives (should ALWAYS be 0) ---")
    print(f"  {results['false_negative']}")

    # -- Stats ------------------------------------------------------------------
    total_non_dict = len(misspelled_words) + len(extra_nonsense)
    fp_count = len(results["false_positive"])
    fp_rate = fp_count / total_non_dict if total_non_dict else 0.0

    print(f"\nMeasured false-positive rate: {fp_count}/{total_non_dict}"
          f" = {fp_rate:.2%}")
    print(f"Filter memory (bits)        : {bf.size}")
    print(f"Set memory estimate (bytes) : ~{len(ground_truth) * 60}"
          "  (rough CPython set overhead)")

    # -- Edge cases -------------------------------------------------------------
    print("\n--- Edge Cases ---")
    empty_bf = build_spell_checker([])
    assert not empty_bf.might_contain("anything"), "Empty filter must reject all"
    print("  Empty dictionary: all words rejected  ✓")

    single_bf = build_spell_checker(["hello"], expected_items=1)
    assert single_bf.might_contain("hello"), "Single-word dict must find it"
    print("  Single-word dict: 'hello' found       ✓")

    case_bf = build_spell_checker(["Hello", "WORLD"])
    assert case_bf.might_contain("hello") and case_bf.might_contain("world")
    print("  Case insensitive: 'Hello'/'WORLD'     ✓")

    print("\nAll checks passed ✓")
