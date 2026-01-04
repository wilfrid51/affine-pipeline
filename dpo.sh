accelerate launch train_dpo_safe_format.py \
  --model /path/to/your_qwen3_4b_chat \
  --train_jsonl dpo_train.jsonl \
  --out_dir dpo_lora_format_safe \
  --use_4bit