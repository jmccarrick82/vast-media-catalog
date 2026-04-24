"""Shared hash comparison utilities for perceptual and cryptographic hashes."""


def hamming_distance(hash_a: str, hash_b: str) -> int:
    """Compute Hamming distance between two hex hash strings.

    Compares character-by-character, XORing nibbles and counting set bits.
    Returns max possible distance if inputs are invalid or different lengths.
    """
    if not hash_a or not hash_b or len(hash_a) != len(hash_b):
        return max(len(hash_a or ""), len(hash_b or "")) * 4
    distance = 0
    for a, b in zip(hash_a, hash_b):
        xor = int(a, 16) ^ int(b, 16)
        distance += bin(xor).count("1")
    return distance


def compare_video_phashes(phash_a: str, phash_b: str) -> float | None:
    """Compare two pipe-separated video perceptual hashes frame-by-frame.

    Returns average Hamming distance across matched frames, or None if
    hashes are missing or have no common frames.
    """
    if not phash_a or not phash_b:
        return None
    frames_a = phash_a.split("|")
    frames_b = phash_b.split("|")
    pairs = min(len(frames_a), len(frames_b))
    if pairs == 0:
        return None
    total = sum(hamming_distance(fa, fb) for fa, fb in zip(frames_a, frames_b))
    return total / pairs


def compare_frame_sequences(phash_a: str, phash_b: str) -> tuple[float, int, int] | None:
    """Detailed frame-by-frame comparison with match counting.

    Returns (avg_distance, frame_matches, total_frames) or None if hashes are missing.
    A frame is considered a match if Hamming distance <= 15.
    """
    if not phash_a or not phash_b:
        return None
    frames_a = phash_a.split("|")
    frames_b = phash_b.split("|")
    pairs = min(len(frames_a), len(frames_b))
    if pairs == 0:
        return None
    total_dist = 0
    matches = 0
    for fa, fb in zip(frames_a, frames_b):
        d = hamming_distance(fa, fb)
        total_dist += d
        if d <= 15:
            matches += 1
    return (total_dist / pairs, matches, pairs)
