# Contribution Report - Blendi Rrustemi


## Application setup and interface

For the backend we used Flask with a browser interface and PostgreSQL for storage. The page has a chat window, and all prompts are saved in PostgreSQL. Each refresh starts with a fresh chat while retaining the previous records in the database.

## Kafka intake and deployment

Each submitted prompt from the browser is published to Kafka using a validated event contract, and an intake consumer saves it to PostgreSQL. The API acknowledges submissions only after Kafka confirms delivery. Stable prompt identifiers and duplicate checks allow retries without creating duplicate records or resetting existing decisions. Docker images, Compose services, and initialization scripts connected the components into a runnable local stack; download timeout settings were also adjusted to improve build reliability.

## P1/P2: preprocessing and source enrichment

P1 produces a Unicode-normalized, whitespace cleaned prompt and it gets the character count, word count, and symbol ratio. The original text will be unchanged for classification and agent execution, and it preserves the exact submitted input.

P2 adds source context using allow, block, and review counts from the preceding ten minutes. To reduce repeated database queries we have a 30-second cache. To the history, only finalized guard decisions contribute and it excludes pending requests and dataset labels. The preprocessing and enrichment snapshot is stored atomically with intake data and saved on duplicate delivery.

## Dataset replay

Imported dataset prompts are sent through a replay tool to the same Kafka path as browser submissions. Configurable rates, simulated sources, and dataset filters support controlled traffic scenarios. Each replayed submission receives a fresh prompt identifier and retains its dataset link for evaluation. Ground-truth labels remain outside streaming events to avoid influencing predictions.

## LangChain agent development and integration

The existing LangChain/Ollama agent was extended and connected to the complete guarded prompt-to-answer flow. Changes added a recursion limit for the model/tool loop, structured JSON output and subprocess execution with a timeout. Atomic database claims prevent duplicate agent execution, while lifecycle tracking records running, completed, failed, and interrupted runs. Model availability checks and worker readiness checks coordinate startup.

Browser polling and a guarded HTTP command-line client expose processing states, guard outcomes, and stored answers. Additional routing safeguards send oversized prompts, failed inference, and missing throttling outcomes to review. In particular, prompts exceeding the classifier's token window are held so that unchecked text cannot reach the agent.

## Testing and documentation

Automated tests were added for API behavior, Kafka intake, preprocessing, source caching, dataset replay, routing, browser polling, and agent lifecycle handling. Complete flow tests use a deterministic model and agent fixtures with Kafka and PostgreSQL to check the routing outcomes and duplicate execution prevention.
