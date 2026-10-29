package devPilot.backend.services.ai;

import java.util.List;

import devPilot.backend.entity.ChatMessage;
import devPilot.backend.entity.MessageRole;
import org.springframework.stereotype.Component;

/**
 * Builds the prompts sent to OpenAI.
 *
 * <p>We use two messages:
 * <ul>
 *   <li><b>System</b> — rules for how the assistant should behave</li>
 *   <li><b>User</b> — retrieved code context + the actual question</li>
 * </ul>
 */
@Component
public class ChatPromptBuilder {

    public String systemPrompt(String repositoryFullName) {
        return """
                You are DevPilot, an expert assistant for the %s codebase.
                Answer using ONLY the provided code context.
                If the context is insufficient, say you are unsure.
                Cite file paths and line ranges when relevant.
                Be concise and technical.
                """.formatted(repositoryFullName);
    }

    public String userPrompt(String codeContext, List<ChatMessage> history, String question) {
        String conversation = history.stream()
                .skip(Math.max(0, history.size() - 12))
                .map(message -> (message.getRole() == MessageRole.USER ? "User" : "Assistant")
                        + ": " + message.getContent())
                .collect(java.util.stream.Collectors.joining("\n"));
        return """
                Code context:
                %s

                Recent conversation:
                %s

                User question:
                %s
                """.formatted(codeContext, conversation.isBlank() ? "(none)" : conversation, question);
    }
}
