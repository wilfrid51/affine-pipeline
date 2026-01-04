# train_dpo_safe_format.py
import argparse
from datasets import load_dataset
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
from peft import LoraConfig, get_peft_model
from trl import DPOConfig, DPOTrainer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True, help="Base model name or path (your Qwen3-style 4B)")
    ap.add_argument("--train_jsonl", required=True, help="DPO train JSONL from the builder script")
    ap.add_argument("--eval_jsonl", default=None, help="Optional eval JSONL")
    ap.add_argument("--out_dir", default="dpo_out")
    ap.add_argument("--use_4bit", action="store_true")
    args = ap.parse_args()

    tokenizer = AutoTokenizer.from_pretrained(args.model, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    quant_cfg = None
    if args.use_4bit:
        quant_cfg = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_use_double_quant=True,
            bnb_4bit_compute_dtype="bfloat16",
        )

    model = AutoModelForCausalLM.from_pretrained(
        args.model,
        trust_remote_code=True,
        device_map="auto",
        quantization_config=quant_cfg,
        torch_dtype="bfloat16",
    )

    # LoRA: keeps base model intact -> minimal drift
    lora_cfg = LoraConfig(
        r=16,
        lora_alpha=32,
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
        # Common Qwen/Llama-style module names. If yours differs, adjust.
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj"],
    )
    model = get_peft_model(model, lora_cfg)

    train_ds = load_dataset("json", data_files=args.train_jsonl, split="train")
    eval_ds = None
    if args.eval_jsonl:
        eval_ds = load_dataset("json", data_files=args.eval_jsonl, split="train")

    # Conservative, format-focused, low-drift settings
    # - Higher beta tends to keep policy closer to reference
    # - label_smoothing makes DPO less aggressive on possibly-noisy prefs
    # - Add "sft" loss to anchor distribution + learn exact format
    dpo_args = DPOConfig(
        output_dir=args.out_dir,

        # lengths: set to your real rollout lengths
        max_prompt_length=2048,
        max_length=3072,

        # DPO knobs (conservative)
        beta=0.4,
        label_smoothing=0.05,

        # Multi-loss: DPO + SFT anchor (supported by TRL) :contentReference[oaicite:5]{index=5}
        loss_type=["sigmoid", "sft"],
        # Put more weight on "sft" to avoid performance drift; increase sigmoid weight if too weak
        loss_weights=[0.2, 1.0],  # if feels weak then increase to [0.4, 1.0]

        # gentle optimization
        learning_rate=5e-5,
        num_train_epochs=1,
        per_device_train_batch_size=1,
        gradient_accumulation_steps=16,
        warmup_ratio=0.05,
        lr_scheduler_type="cosine",

        bf16=True,
        gradient_checkpointing=True,

        logging_steps=10,
        save_steps=200,
        eval_steps=200,
        evaluation_strategy=("steps" if eval_ds is not None else "no"),
        save_total_limit=2,
        report_to="none",
    )

    trainer = DPOTrainer(
        model=model,
        args=dpo_args,
        processing_class=tokenizer,  # TRL quickstart API :contentReference[oaicite:6]{index=6}
        train_dataset=train_ds,
        eval_dataset=eval_ds,
    )

    trainer.train()
    trainer.save_model(args.out_dir)
    tokenizer.save_pretrained(args.out_dir)

if __name__ == "__main__":
    main()
