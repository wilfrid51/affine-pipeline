python build_dpo_from_env_and_sft.py \
  --env_jsonl env_episodes.jsonl \
  --sft_jsonl sft_data.jsonl \
  --max_sft 50000 \
  --out_jsonl dpo_train.jsonl