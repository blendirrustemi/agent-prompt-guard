const form = document.querySelector("#prompt-form");
const input = document.querySelector("#prompt");
const sendButton = document.querySelector("#send-button");
const counter = document.querySelector("#char-count");
const error = document.querySelector("#form-error");
const messages = document.querySelector("#messages");
const workspace = document.querySelector("#workspace");
const composerArea = document.querySelector(".composer-area");
const intro = document.querySelector("#intro");
const maxLength = Number(form.dataset.maxLength);
let sending = false;
let submission = null;

function refreshInput() {
    const length = Array.from(input.value).length;
    counter.textContent = `${length.toLocaleString()} / ${maxLength.toLocaleString()}`;
    counter.classList.toggle("over-limit", length > maxLength);
    sendButton.disabled = sending || !input.value.trim() || length > maxLength;
    input.style.height = "auto";
    input.style.height = `${Math.min(input.scrollHeight, 200)}px`;
}

function showError(message = "") {
    error.textContent = message;
    error.hidden = !message;
}

function addPrompt(prompt, origin, deliveryStatus) {
    const firstPrompt = !workspace.classList.contains("has-messages");
    const animateTransition = firstPrompt && typeof form.animate === "function"
        && !window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    let fadingIntro;
    if (animateTransition) {
        const rect = intro.getBoundingClientRect();
        fadingIntro = intro.cloneNode(true);
        fadingIntro.removeAttribute("id");
        fadingIntro.classList.add("intro-transition");
        fadingIntro.setAttribute("aria-hidden", "true");
        fadingIntro.inert = true;
        Object.assign(fadingIntro.style, {
            left: `${rect.left}px`, top: `${rect.top}px`, width: `${rect.width}px`,
        });
        document.body.append(fadingIntro);
    }
    const article = document.createElement("article");
    article.className = "message";
    article.dataset.promptId = prompt.id;
    const text = document.createElement("p");
    text.className = "message-text";
    text.textContent = prompt.text;
    const meta = document.createElement("div");
    meta.className = "message-meta";
    const time = document.createElement("time");
    time.dateTime = prompt.created_at;
    time.textContent = new Date(prompt.created_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    const status = document.createElement("span");
    status.className = "decision";
    const labels = { pending: "Pending check", allow: "Allowed", block: "Blocked", review: "Needs review" };
    status.textContent = deliveryStatus === "queued" ? "Queued" : labels[prompt.decision] || "Pending check";
    meta.append(time, status);
    const reply = document.createElement("div");
    reply.className = "message-response";
    reply.hidden = true;
    const replyLabel = document.createElement("p");
    replyLabel.className = "response-label";
    const replyText = document.createElement("p");
    replyText.className = "response-text";
    reply.append(replyLabel, replyText);
    article.append(text, meta, reply);
    messages.append(article);
    workspace.classList.add("has-messages");
    messages.scrollTop = messages.scrollHeight;
    const view = { article, status, reply, replyLabel, replyText };
    setTimeout(() => followResult(prompt.id, view), 2_000);

    if (animateTransition) {
        const composerTarget = form.getBoundingClientRect();
        const messageTarget = text.getBoundingClientRect();
        const motion = { duration: 700, easing: "cubic-bezier(0.22, 1, 0.36, 1)" };
        composerArea.animate([
            { transform: `translate(${origin.composer.left - composerTarget.left}px, ${origin.composer.top - composerTarget.top}px)` },
            { transform: "translate(0, 0)" },
        ], motion);
        article.animate([
            { transform: `translate(${origin.input.left - messageTarget.left}px, ${origin.input.top - messageTarget.top}px)`, opacity: 0 },
            { opacity: 1, offset: 0.2 },
            { transform: "translate(0, 0)", opacity: 1 },
        ], motion);
        meta.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 250, delay: 350, fill: "backwards" });
        const fade = fadingIntro.animate([
            { transform: "translateY(0)", opacity: 1 },
            { transform: "translateY(-20px)", opacity: 0 },
        ], { duration: 250, easing: "ease-out", fill: "forwards" });
        fade.finished.then(() => fadingIntro.remove(), () => fadingIntro.remove());
    }
}

function renderResult(result, view) {
    const { article, status, reply, replyLabel, replyText } = view;
    const state = result.state || { allow: "agent_queued", block: "blocked", review: "review" }[result.prompt.decision] || "checking";
    const labels = {
        checking: "Checking", agent_queued: "Waiting for answer", agent_running: "Answering",
        completed: "Allowed", blocked: "Blocked", review: "Needs review", agent_failed: "Answer failed",
    };
    article.dataset.state = state;
    if (status.textContent !== labels[state]) status.textContent = labels[state] || "Checking";

    let text = "";
    let label = "Prompt Guard";
    if (state === "agent_queued" || state === "agent_running") {
        label = "Assistant";
        text = state === "agent_running" ? "Preparing your answer…" : "Your prompt was allowed. Waiting for the assistant…";
    } else if (state === "completed") {
        label = "Assistant";
        text = result.agent?.response || "The assistant returned an empty response. Please try again.";
    } else if (state === "blocked") {
        text = result.guard?.reason?.startsWith("throttle_")
            ? "Too many requests were received from this connection. Please wait before trying again."
            : "The guard blocked this prompt, so it wasn’t sent to the assistant.";
    } else if (state === "review") {
        const reason = result.guard?.reason;
        if (reason === "model_input_too_long") {
            text = "This prompt is too long for the guard to check completely. Shorten it and try again.";
        } else if (reason === "score_in_review_band") {
            text = "The guard was uncertain about this prompt. It was held for review and wasn’t sent to the assistant.";
        } else {
            text = "The guard couldn’t complete its checks. This prompt was held for review and wasn’t sent to the assistant.";
        }
    } else if (state === "agent_failed") {
        label = "Assistant";
        text = "The assistant couldn’t complete this request. Please try again.";
    }
    reply.hidden = !text;
    if (replyLabel.textContent !== label) replyLabel.textContent = label;
    // Render model output as text, never HTML supplied by the model or prompt.
    if (replyText.textContent !== text) replyText.textContent = text;
    const pending = state === "agent_queued" || state === "agent_running";
    reply.classList.toggle("is-pending", pending);
    reply.classList.toggle("is-notice", state === "blocked" || state === "review" || state === "agent_failed");
    reply.setAttribute("aria-busy", String(pending));
    return ["completed", "blocked", "review", "agent_failed"].includes(state);
}

async function followResult(promptId, view) {
    if (!view.article.isConnected) return;
    try {
        const result = await api(`/api/prompts/${promptId}`);
        if (!view.article.isConnected) return;
        const wasAtBottom = messages.scrollHeight - messages.scrollTop - messages.clientHeight < 80;
        const finished = renderResult(result, view);
        if (wasAtBottom) messages.scrollTop = messages.scrollHeight;
        if (finished) return;
    } catch {
        // Storage/API outages preserve the last state and never resubmit the prompt.
    }
    if (view.article.isConnected) setTimeout(() => followResult(promptId, view), 2_000);
}

async function api(url, options = {}) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10_000);
    try {
        const response = await fetch(url, { ...options, signal: controller.signal });
        const body = await response.json();
        if (!response.ok) throw new Error(body.error || "Something went wrong. Please try again.");
        return body;
    } finally {
        clearTimeout(timer);
    }
}

input.addEventListener("input", () => {
    showError();
    refreshInput();
});

input.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
        event.preventDefault();
        if (!sendButton.disabled) form.requestSubmit();
    }
});

form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (sendButton.disabled) return;
    sending = true;
    input.readOnly = true;
    sendButton.setAttribute("aria-label", "Queuing prompt");
    form.setAttribute("aria-busy", "true");
    refreshInput();
    showError();
    try {
        if (!submission || submission.text !== input.value) {
            submission = { prompt_id: crypto.randomUUID(), text: input.value };
        }
        const { prompt, status } = await api("/api/prompts", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(submission),
        });
        const origin = {
            composer: form.getBoundingClientRect(),
            input: input.getBoundingClientRect(),
        };
        input.value = "";
        submission = null;
        refreshInput();
        addPrompt(prompt, origin, status);
    } catch (failure) {
        const message = failure.name === "AbortError" || failure instanceof TypeError
            ? "Could not confirm that your prompt was queued. Check your connection and retry."
            : failure.message;
        showError(message);
    } finally {
        sending = false;
        input.readOnly = false;
        sendButton.setAttribute("aria-label", "Send prompt");
        form.removeAttribute("aria-busy");
        refreshInput();
        input.focus({ preventScroll: true });
    }
});

form.reset();
refreshInput();
