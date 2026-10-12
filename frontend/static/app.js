const form = document.querySelector("#prompt-form");
const input = document.querySelector("#prompt");
const sendButton = document.querySelector("#send-button");
const counter = document.querySelector("#char-count");
const error = document.querySelector("#form-error");
const messages = document.querySelector("#messages");
const workspace = document.querySelector("#workspace");
const maxLength = Number(form.dataset.maxLength);
let sending = false;
let loading = true;

function refreshInput() {
    const length = Array.from(input.value).length;
    counter.textContent = `${length.toLocaleString()} / ${maxLength.toLocaleString()}`;
    counter.classList.toggle("over-limit", length > maxLength);
    sendButton.disabled = loading || sending || !input.value.trim() || length > maxLength;
    input.style.height = "auto";
    input.style.height = `${Math.min(input.scrollHeight, 200)}px`;
}

function showError(message = "") {
    error.textContent = message;
    error.hidden = !message;
}

function addPrompt(prompt) {
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
    status.textContent = labels[prompt.decision] || "Pending check";
    meta.append(time, status);
    article.append(text, meta);
    messages.append(article);
    workspace.classList.add("has-messages");
    messages.scrollTop = messages.scrollHeight;
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
    sendButton.setAttribute("aria-label", "Saving prompt");
    form.setAttribute("aria-busy", "true");
    refreshInput();
    showError();
    try {
        const { prompt } = await api("/api/prompts", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ text: input.value }),
        });
        addPrompt(prompt);
        input.value = "";
    } catch (failure) {
        const message = failure.name === "AbortError" || failure instanceof TypeError
            ? "Could not confirm that your prompt was saved. Check your connection and try again."
            : failure.message;
        showError(message);
    } finally {
        sending = false;
        input.readOnly = false;
        sendButton.setAttribute("aria-label", "Send prompt");
        form.removeAttribute("aria-busy");
        refreshInput();
        input.focus();
    }
});

async function loadPrompts() {
    try {
        const { prompts } = await api("/api/prompts");
        prompts.forEach(addPrompt);
    } catch {
        showError("Could not load saved prompts. Check that local storage is running, then refresh.");
    } finally {
        loading = false;
        refreshInput();
    }
}

loadPrompts();
