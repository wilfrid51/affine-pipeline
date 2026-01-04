# build_dpo_from_env_and_sft.py
import argparse, json
from typing import Any, Dict, List, Optional, Tuple

Msg = Dict[str, str]  # {"role": "...", "content": "..."}

def longest_common_prefix(a: List[Msg], b: List[Msg]) -> int:
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i].get("role") == b[i].get("role") and a[i].get("content") == b[i].get("content"):
        i += 1
    return i

def extract_first_divergence_pair(
    good: List[Msg], bad: List[Msg]
) -> Optional[Tuple[List[Msg], str, str, int]]:
    """
    Returns: (prompt_messages, chosen_text, rejected_text, step_index)
    step_index is the index in the history where assistant diverged.
    """
    k = longest_common_prefix(good, bad)
    if k >= len(good) or k >= len(bad):
        return None

    # We need the next message in each history to be an assistant turn to form a proper DPO pair
    g_next = good[k]
    b_next = bad[k]
    if g_next.get("role") != "assistant" or b_next.get("role") != "assistant":
        # If divergence happens on user/env message, prompts differ and it is not a valid DPO comparison.
        return None

    prompt_messages = good[:k]  # common prefix (same as bad[:k])
    chosen_text = g_next.get("content", "")
    rejected_text = b_next.get("content", "")
    if not chosen_text or not rejected_text:
        return None
    return prompt_messages, chosen_text, rejected_text, k

def normalize_sft_row(row: Dict[str, Any]) -> Optional[Tuple[List[Msg], str]]:
    """
    Accepts either:
      - {"messages":[...]} where last message is assistant
      - {"prompt":[...], "completion":[{"role":"assistant","content":"..."}]}
      - {"prompt":[...], "completion":"..."} (string completion)
    Returns (prompt_messages, chosen_text)
    """
    if "messages" in row and isinstance(row["messages"], list) and row["messages"]:
        msgs = row["messages"]
        if msgs[-1].get("role") != "assistant":
            return None
        prompt = msgs[:-1]
        chosen = msgs[-1].get("content", "")
        return (prompt, chosen) if chosen else None

    if "prompt" in row and "completion" in row and isinstance(row["prompt"], list):
        prompt = row["prompt"]
        comp = row["completion"]
        if isinstance(comp, list) and comp and comp[-1].get("role") == "assistant":
            chosen = comp[-1].get("content", "")
            return (prompt, chosen) if chosen else None
        if isinstance(comp, str) and comp.strip():
            return (prompt, comp.strip())

    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env_jsonl", required=True, help="JSONL of episodes with good/bad histories and scores")
    ap.add_argument("--out_jsonl", required=True, help="Output DPO JSONL")
    ap.add_argument("--sft_jsonl", default=None, help="Optional SFT JSONL to use as anchors")
    ap.add_argument("--max_sft", type=int, default=0, help="How many SFT anchors to add (0 = none)")
    args = ap.parse_args()

    out = open(args.out_jsonl, "w", encoding="utf-8")

    kept_env = 0
    skipped_env = 0

    with open(args.env_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            ep = json.loads(line)

            good_score = float(ep.get("good_score", 1.0))
            bad_score = float(ep.get("bad_score", 0.0))
            good_hist = ep.get("good_history")
            bad_hist = ep.get("bad_history")
            if not isinstance(good_hist, list) or not isinstance(bad_hist, list):
                skipped_env += 1
                continue

            # Ensure we assign chosen to the higher score trajectory
            if bad_score > good_score:
                good_score, bad_score = bad_score, good_score
                good_hist, bad_hist = bad_hist, good_hist

            pair = extract_first_divergence_pair(good_hist, bad_hist)
            if pair is None:
                skipped_env += 1
                continue

            prompt_msgs, chosen, rejected, step_idx = pair
            rec = {
                "prompt": prompt_msgs,
                "chosen": [{"role": "assistant", "content": chosen}],
                "rejected": [{"role": "assistant", "content": rejected}],
                "chosen_score": good_score,
                "rejected_score": bad_score,
                "meta": {
                    "episode_id": ep.get("episode_id"),
                    "task_id": ep.get("task_id"),
                    "divergence_step": step_idx,
                },
            }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            kept_env += 1

    kept_sft = 0
    if args.sft_jsonl and args.max_sft > 0:
        with open(args.sft_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                if kept_sft >= args.max_sft:
                    break
                row = json.loads(line)
                norm = normalize_sft_row(row)
                if norm is None:
                    continue
                prompt_msgs, chosen = norm

                # Anchor row: rejected == chosen (no preference signal, but we’ll enable SFT loss later)
                rec = {
                    "prompt": prompt_msgs,
                    "chosen": [{"role": "assistant", "content": chosen}],
                    "rejected": [{"role": "assistant", "content": chosen}],
                    "meta": {
                        "anchor": True,
                        "source": "sft",
                        "task_id": row.get("task_id"),
                    },
                }
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                kept_sft += 1

    out.close()
    print(f"Wrote {kept_env} env DPO pairs (skipped {skipped_env}). Added {kept_sft} SFT anchors.")

if __name__ == "__main__":
    main()
