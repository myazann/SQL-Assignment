import tiktoken

MAX_TOKENS = 16000

SYSTEM_PROMPT = (
    "You are a helpful, source-aware assistant. "
    "Use web search for fresh/niche/verify-able facts. "
    "When you use search, include brief citations as links."
)

SQL_SYSTEM_PROMPT = (
    "You are a helpful assistant that answers questions about PostgreSQL databases. "
    "When you use search, include brief citations as links. "
    "Use markdowns to separate the code and the text in your output. "
)

def build_input_from_history(message, history):
    parts = []
    parts.append({"role": "system", "content": SQL_SYSTEM_PROMPT})
    # prior turns
    for msg in history:
        if msg["role"] == "user":
            parts.append({"role": "user", "content": msg["content"]})
        if msg["role"] == "assistant":
            parts.append({"role": "assistant", "content": msg["content"]})
    parts.append({"role": "user", "content": message})

    parts = truncate_history(parts, MAX_TOKENS)

    return parts

def count_tokens(messages, model="gpt-4.1"):
    """Count tokens in a list of messages using tiktoken."""
    try:
        enc = tiktoken.encoding_for_model(model)
    except KeyError:
        enc = tiktoken.get_encoding("cl100k_base")
    num_tokens = 0
    for msg in messages:
        num_tokens += len(enc.encode(msg["content"]))
    return num_tokens

def truncate_history(messages, max_tokens=MAX_TOKENS, model="gpt-4.1"):
    """Trim history from the oldest messages if over context window."""
    while count_tokens(messages, model=model) > max_tokens and len(messages) > 2:
        # remove the second message (first after system prompt)
        messages.pop(1)
    return messages