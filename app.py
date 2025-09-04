import os
import gradio as gr
from openai import AsyncOpenAI
import pandas as pd
import uuid, asyncio

from sql_tab import run_sql
from logger import log_event
from chat_helpers import build_input_from_history, get_db_sys_prompt

oclient = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
max_rows = 100

async def respond_once(message, history):

    text_input = build_input_from_history(message, history)
    kwargs = dict(
        model="gpt-4.1",
        input=text_input,
        instructions=get_db_sys_prompt(),
        tools=[{"type": "web_search"}],
        tool_choice="auto",
        parallel_tool_calls=True,
    )
    # MOCK mode to isolate app/DB without burning tokens
    if os.getenv("MOCK_OPENAI", "").lower() in {"1", "true", "yes"}:
        import random, asyncio
        await asyncio.sleep(random.uniform(0.05, 0.25))
        return "MOCK: Here’s a fabricated answer for load testing."

    resp = await oclient.responses.create(**kwargs)
    return getattr(resp, "output_text", "")

async def respond(message, history):
    text_input = build_input_from_history(message, history)
    kwargs = dict(
        model="gpt-4.1",
        input=text_input,
        instructions=get_db_sys_prompt(),
        tools=[{"type": "web_search"}],
        tool_choice="auto",
        parallel_tool_calls=True,
    )

    buffer = []
    async with oclient.responses.stream(**kwargs) as stream:
        async for event in stream:
            if event.type == "response.output_text.delta":
                buffer.append(event.delta)
                yield "".join(buffer)

        final = await stream.get_final_response()
        final_text = getattr(final, "output_text", None)
        if final_text and (not buffer or final_text != "".join(buffer)):
            yield final_text

async def chat_driver(user_message, messages_history, _user_name, _session_id):
    messages_history = messages_history or []
    base = messages_history + [{"role": "user", "content": user_message}]
    assistant_text = ""

    asyncio.create_task(log_event(_user_name, _session_id, "chat_user", {"text": user_message}))

    async for chunk in respond(user_message, messages_history):
        assistant_text = chunk
        # stream to UI
        yield base + [{"role": "assistant", "content": assistant_text}], ""

    # after stream finished, log the final assistant text
    asyncio.create_task(log_event(_user_name, _session_id, "chat_assistant", {"text": assistant_text}))

async def post_completion_code(_user_name, _session_id):
    code = "9C1F4B2E"
    msg = f"the completion code is {code}"
    updated = [{"role": "assistant", "content": msg}]

    await log_event(_user_name, _session_id, "completion_code", {"code": code})
    return updated

with gr.Blocks(title="Movie Database", theme="soft") as demo:
    # gr.Markdown("## Movie Database Bot and SQL Console")
    user_name = gr.State("")
    session_id = gr.State("") 

    with gr.Column(visible=True) as identify_view:
        gr.Markdown("### Login")
        name_tb = gr.Textbox(label="Your name (required)", placeholder="Please enter your full name, including your last name", autofocus=True)
        enter_btn = gr.Button("Enter", variant="primary")
        id_msg = gr.Markdown("") 

    async def do_login(name):
        name = (name or "").strip()
        if not name:
            return (gr.update(visible=True), gr.update(visible=False), "⚠️ Please enter your name to continue.", "", "")
        sid = uuid.uuid4().hex
        await log_event(name, sid, "login", {"meta": {"agent": "gradio_app", "version": 1}})
        return (gr.update(visible=False), gr.update(visible=True), "", name, sid)

    with gr.Column(visible=False) as app_view:

        welcome_md = gr.Markdown("") 
        with gr.Tabs():
            with gr.Tab("Usage"):
                gr.Markdown("""
                <h2> Platform Usage and the Assignment </h2>
                <br>
                <ul>
                    <li>You can use the SQL tab to check if you have the correct query for the question asked in the survey.</li>
                    <li>The chat tab provides you a chatbot to ask questions about PostgreSQL, which is backed up by GPT-4.1.</li>
                    <li>You are not constrained to use the chatbot, but you have to generate a completion code to move onto the next question. The code can be generated via the chatbot.</li>
                    <li> <b> Reminder: </b> This assignment is optional and ungraded. It is designed for you to practice. You can be relaxed, it is okay to have errors. Good luck! </li>
                </ul>

                <h3> Database </h3>

                The database have four tables, each corresponding to the 4 excel files you have for the project:
                <ul>
                    <li>sales</li>
                    <li>metadata</li>
                    <li>user_reviews</li>
                    <li>expert_reviews</li>
                </ul>
                <br>
                Each table has all the columns of the excel files, except for the "Rev" columns, which are the review texts.
                <br>
                <br>
                <b> Important Notes: </b> 
                <br>
                <br>
                <ul>
                    <li> Check the datatypes of the columns. A column that stores a numerical information might have the datatype "text". </li>
                    <li> Datatypes might not be exactly the same as the excel files. </li>
                    <li> Some columns might store null values as text, like "n/a" or "null". </li>
                    <li> Columns with same names might store different values in different tables. Example: "url" column in metadata and sales.</li>
                    <li> A proper ERD or foreign key relationships are not defined for the tables. You can still join tables based on the column names, but be careful. </li>
                </ul>
                """)

            with gr.Tab("SQL"):
                with gr.Column(): 
                    sql_input = gr.Code(
                        label="SQL",
                        language="sql",
                        value="SELECT * FROM sales;",
                        lines=10,
                    )
                    with gr.Row():
                        run_btn = gr.Button("Run", variant="primary")
                        clear_btn = gr.Button("Clear")

                    results = gr.Dataframe(
                        label="Results",
                        wrap=True,
                        interactive=False,
                    )
                    meta = gr.Markdown("Ready.")
                    plan = gr.Markdown("", label="Explain/Plan") 

            async def on_run(q, _user_name, _session_id):
                df, meta_msg, _ = await asyncio.to_thread(run_sql, q, max_rows, False)

                await log_event(
                    _user_name, _session_id, "sql",
                    {
                        "query": q,
                        "row_limit": max_rows,
                        "row_count": int(getattr(df, "shape", [0])[0]),
                        "meta": meta_msg,
                    },
                )
                return df, meta_msg, ""

            def on_clear():
                return "", pd.DataFrame(), "Cleared.", ""

            run_btn.click(on_run, [sql_input, user_name, session_id], [results, meta, plan])

            with gr.Tab("Chat"):
                chatbot = gr.Chatbot(type="messages", label="Conversation", height=450)

                with gr.Row():
                    chat_input = gr.Textbox(
                        placeholder="How can I help you with PostgreSQL today?",
                        scale=8,
                        autofocus=True,
                        container=False,
                    )
                    send_btn = gr.Button("Send", variant="primary", scale=1)
                    code_btn = gr.Button("Completion code", variant="secondary", scale=1)

                def _clear_input():
                    return ""

                ev = send_btn.click(chat_driver, [chat_input, chatbot, user_name, session_id], [chatbot, chat_input])
                ev.then(_clear_input, None, [chat_input])

                ev2 = chat_input.submit(chat_driver, [chat_input, chatbot, user_name, session_id], [chatbot, chat_input])
                ev2.then(_clear_input, None, [chat_input])

                code_btn.click(
                    post_completion_code,
                    inputs=[user_name, session_id],
                    outputs=[chatbot],
                )
        
        outputs = [identify_view, app_view, id_msg, user_name, session_id]
        enter_btn.click(do_login, inputs=[name_tb], outputs=outputs)
        name_tb.submit(do_login, inputs=[name_tb], outputs=outputs)

        def greet(name):
            return f"**Hello, {name}!**"
        user_name.change(greet, inputs=[user_name], outputs=[welcome_md])


if __name__ == "__main__":
    demo.launch(share=True)
