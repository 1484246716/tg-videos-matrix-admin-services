import { AiModelProfile } from '@prisma/client';
import OpenAI from 'openai';

/**
 * Generate text using the provided AI profile and prompt messages.
 */
export async function generateTextWithAiProfile(
    profile: Pick<AiModelProfile, 'endpointUrl' | 'apiKeyEncrypted' | 'model' | 'temperature' | 'topP' | 'maxTokens'>,
    systemPrompt: string | null | undefined,
    userPrompt: string
): Promise<string> {
    let baseURL = profile.endpointUrl && profile.endpointUrl.trim() !== '' ? profile.endpointUrl.trim() : undefined;
    if (baseURL && baseURL.endsWith('/chat/completions')) {
        baseURL = baseURL.replace(/\/chat\/completions\/?$/, '');
    }

    const client = new OpenAI({
        apiKey: profile.apiKeyEncrypted,
        baseURL,
    });

    const messages: OpenAI.Chat.ChatCompletionMessageParam[] = [];

    if (systemPrompt && systemPrompt.trim() !== '') {
        messages.push({
            role: 'system',
            content: systemPrompt.trim(),
        });
    }

    messages.push({
        role: 'user',
        content: userPrompt,
    });

    const response = await client.chat.completions.create({
        model: profile.model,
        messages,
        temperature: profile.temperature ? Number(profile.temperature) : undefined,
        top_p: profile.topP ? Number(profile.topP) : undefined,
        max_tokens: profile.maxTokens ?? undefined,
    });

    const content = response.choices[0]?.message?.content;
    if (!content) {
        throw new Error('AI 模型返回了空内容');
    }

    return content;
}
