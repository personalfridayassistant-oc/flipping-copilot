package com.flippingcopilot.controller;

import com.flippingcopilot.model.*;
import com.flippingcopilot.rs.CopilotLoginRS;
import com.flippingcopilot.ui.graph.model.Data;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import net.runelite.client.callback.ClientThread;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;


@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class ApiRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiRequestHandler.class);
    private static final String serverUrl = "http://192.168.1.27";
    private static final String serverFeUrl = serverUrl;
    private static final String runeliteSuggestionsUrl = serverUrl + "/api/v1/suggestions/runelite?limit=25";
    public static final String DEFAULT_COPILOT_PRICE_ERROR_MESSAGE = "Unable to fetch price copilot price (possible server update)";
    public static final String DEFAULT_PREMIUM_INSTANCE_ERROR_MESSAGE = "Error loading premium instance data (possible server update)";
    public static final String UNKNOWN_ERROR = "Unknown error";
    public static final int UNAUTHORIZED_CODE = 401;
    // dependencies
    private final OkHttpClient client;
    private final OkHttpClient localSuggestionClient = new OkHttpClient.Builder()
            .proxy(java.net.Proxy.NO_PROXY)
            .connectionSpecs(java.util.Collections.singletonList(ConnectionSpec.CLEARTEXT))
            .build();
    private final Gson gson;
    private final CopilotLoginRS copilotLoginRS;
    private final SuggestionPreferencesManager preferencesManager;
    private final ClientThread clientThread;


    public void authenticate(String username, String password, Consumer<LoginResponse> successCallback, Consumer<String> failureCallback) {
        Request request = new Request.Builder()
                .url(serverUrl + "/login")
                .addHeader("Authorization", Credentials.basic(username, password))
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), ""))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                failureCallback.accept(UNKNOWN_ERROR);
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        if(response.code() == UNAUTHORIZED_CODE) {
                            copilotLoginRS.clear();
                        }
                        log.warn("login failed with http status code {}", response.code());
                        String errorMessage = extractErrorMessage(response);
                        failureCallback.accept(errorMessage);
                        return;
                    }
                    String body = response.body() == null ? "" : response.body().string();
                    LoginResponse loginResponse = gson.fromJson(body, LoginResponse.class);
                    successCallback.accept(loginResponse);
                } catch (IOException | JsonParseException e) {
                    log.warn("error reading/decoding login response body", e);
                    failureCallback.accept(UNKNOWN_ERROR);
                }
            }
        });
    }

    public Call discordLoginAsync(Consumer<String> oathUrlConsumer,
                                  Consumer<LoginResponse> loginResponseConsumer,
                                  Consumer<HttpResponseException>  onFailure) {
        log.debug("sending request to login via discord");
        Request r = new Request.Builder()
                .url(serverFeUrl + "/v1/plugin-discord-login")
                .get().build();

        Call call = client.newBuilder()
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .callTimeout(0, TimeUnit.MILLISECONDS)
                .build()
                .newCall(r);

        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.warn("login via discord call failed", e);
                clientThread.invoke(() -> onFailure.accept(new HttpResponseException(-1, UNKNOWN_ERROR)));
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        if(response.code() == UNAUTHORIZED_CODE) {
                            copilotLoginRS.clear();
                        }
                        log.warn("login via discord call failed with http status code {}", response.code());
                        clientThread.invoke(() -> onFailure.accept(new HttpResponseException(response.code(), extractErrorMessage(response))));
                        return;
                    }
                    if (response.body() == null) {
                        throw new IOException("empty discord login response");
                    }
                    try(DataInputStream is = new DataInputStream(new BufferedInputStream(response.body().byteStream()))) {
                        PluginDiscordLoginInitResponse initResponse = PluginDiscordLoginInitResponse.fromRaw(is);
                        clientThread.invoke(() -> oathUrlConsumer.accept(initResponse.getUrl()));
                        LoginResponse loginResponse = LoginResponse.fromRaw(is);
                        if (loginResponse.getError() != null && !loginResponse.getError().isEmpty()) {
                            clientThread.invoke(() -> onFailure.accept(new HttpResponseException(-1, loginResponse.getError())));
                        } else {
                            clientThread.invoke(() -> loginResponseConsumer.accept(loginResponse));
                        }
                    }
                } catch (Exception e) {
                    log.warn("error reading/parsing discord login response body", e);
                    clientThread.invoke(() -> onFailure.accept(new HttpResponseException(-1, UNKNOWN_ERROR)));
                }
            }
        });

        return call;
    }

    public void getSuggestionAsync(JsonObject status,
                                   Consumer<Suggestion> suggestionConsumer,
                                   Consumer<Data> graphDataConsumer,
                                   Consumer<HttpResponseException>  onFailure,
                                   boolean skipGraphData) {
        log.debug("requesting runelite suggestions from {}", runeliteSuggestionsUrl);
        Request request = new Request.Builder()
                .url(runeliteSuggestionsUrl)
                .addHeader("Accept", "application/json")
                .get()
                .build();

        localSuggestionClient.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                String errorMessage = e.getMessage();
                if (e instanceof javax.net.ssl.SSLException) {
                    errorMessage = "Local API SSL error. Use plain HTTP (not HTTPS): " + runeliteSuggestionsUrl;
                }
                log.warn("call to get suggestion failed", e);
                String finalErrorMessage = (errorMessage == null || errorMessage.isEmpty()) ? UNKNOWN_ERROR : errorMessage;
                clientThread.invoke(() -> onFailure.accept(new HttpResponseException(-1, finalErrorMessage)));
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        log.warn("get suggestion failed with http status code {}", response.code());
                        clientThread.invoke(() -> onFailure.accept(new HttpResponseException(response.code(), extractErrorMessage(response))));
                        return;
                    }
                    handleSuggestionResponse(response, status, suggestionConsumer, graphDataConsumer);
                } catch (Exception e) {
                    log.warn("error reading/parsing suggestion response body", e);
                    clientThread.invoke(() -> onFailure.accept(new HttpResponseException(-1, UNKNOWN_ERROR)));
                }
            }
        });
    }

    private void handleSuggestionResponse(Response response,
                                          JsonObject status,
                                          Consumer<Suggestion> suggestionConsumer,
                                          Consumer<Data> graphDataConsumer) throws IOException {
        if (response.body() == null) {
            throw new IOException("empty suggestion request response");
        }

        String body = response.body().string();
        log.debug("runelite suggestion response size is: {}", body.getBytes().length);
        Suggestion suggestion;
        try {
            suggestion = parseRuneliteSuggestion(body, status);
        } catch (Exception e) {
            log.warn("failed to parse runelite suggestion response", e);
            Suggestion waitSuggestion = new Suggestion();
            waitSuggestion.setType("wait");
            waitSuggestion.setMessage("Unable to parse API response.");
            suggestion = waitSuggestion;
        }

        Suggestion finalSuggestion = suggestion;
        clientThread.invoke(() -> suggestionConsumer.accept(finalSuggestion));
        Data d = new Data();
        d.loadingErrorMessage = "No graph data loaded for this item.";
        clientThread.invoke(() -> graphDataConsumer.accept(d));
    }

    private Suggestion parseRuneliteSuggestion(String body, JsonObject status) {
        JsonElement parsed = new JsonParser().parse(body);
        if (parsed.isJsonObject() && parsed.getAsJsonObject().has("type")) {
            return gson.fromJson(parsed, Suggestion.class);
        }

        JsonArray suggestions = extractSuggestionsArray(parsed);
        if (suggestions.size() == 0) {
            Suggestion waitSuggestion = new Suggestion();
            waitSuggestion.setType("wait");
            waitSuggestion.setMessage("No API suggestions returned.");
            return waitSuggestion;
        }

        boolean isMember = status != null && status.has("is_member") && status.get("is_member").getAsBoolean();
        int freeSlots = inferFreeSlots(status, isMember ? 8 : 3);
        long availableCoins = inferAvailableCoins(status);
        Set<Integer> blockedItems = inferBlockedItems(status);

        JsonObject selected = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (JsonElement e : suggestions) {
            if (!e.isJsonObject()) {
                continue;
            }
            JsonObject candidate = e.getAsJsonObject();
            int itemId = readInt(candidate, "item_id", -1);
            if (itemId < 0 || blockedItems.contains(itemId)) {
                continue;
            }
            if (!isMember && readBoolean(candidate, "members", false)) {
                continue;
            }
            if (freeSlots <= 0) {
                continue;
            }

            int buyPrice = readInt(candidate, "buy_price", readInt(candidate, "buy", 0));
            if (buyPrice <= 0 || buyPrice > availableCoins) {
                continue;
            }

            double score = readDouble(candidate, "score", 0d);
            int minVolume = readInt(candidate, "min_volume", readInt(candidate, "volume", 0));
            score += Math.min(minVolume, 10_000) / 10_000d;
            if (score > bestScore) {
                bestScore = score;
                selected = candidate;
            }
        }

        if (selected == null) {
            Suggestion waitSuggestion = new Suggestion();
            waitSuggestion.setType("wait");
            waitSuggestion.setMessage("No valid suggestion for current account restrictions.");
            return waitSuggestion;
        }

        int buyPrice = readInt(selected, "buy_price", readInt(selected, "buy", 0));
        int quantity = Math.max(1, (int) Math.min(availableCoins / Math.max(buyPrice, 1), 10_000));
        int sellPrice = readInt(selected, "sell_price", readInt(selected, "sell", buyPrice));
        double expectedProfit = Math.max(0, (sellPrice - buyPrice) * (double) quantity);

        int minVolume = readInt(selected, "min_volume", readInt(selected, "volume", 0));
        Double roi = readDouble(selected, "roi", null);
        Double score = readDouble(selected, "score", null);
        Suggestion suggestion = new Suggestion();
        suggestion.setType("buy");
        suggestion.setBoxId(0);
        suggestion.setItemId(readInt(selected, "item_id", -1));
        suggestion.setPrice(buyPrice);
        suggestion.setQuantity(quantity);
        suggestion.setName(readString(selected, "name", "Unknown item"));
        suggestion.setId(readInt(selected, "id", -1));
        suggestion.setMessage(String.format("API suggestion (min vol: %d%s%s)",
                minVolume,
                roi == null ? "" : ", roi: " + String.format("%.2f", roi),
                score == null ? "" : ", score: " + String.format("%.2f", score)));
        suggestion.setExpectedProfit(expectedProfit);
        return suggestion;
    }


    private JsonArray extractSuggestionsArray(JsonElement parsed) {
        if (parsed != null && parsed.isJsonArray()) {
            return parsed.getAsJsonArray();
        }
        if (parsed != null && parsed.isJsonObject()) {
            JsonObject obj = parsed.getAsJsonObject();
            if (obj.has("suggestions") && obj.get("suggestions").isJsonArray()) {
                return obj.getAsJsonArray("suggestions");
            }
            if (obj.has("data") && obj.get("data").isJsonArray()) {
                return obj.getAsJsonArray("data");
            }
        }
        return new JsonArray();
    }

    private int inferFreeSlots(JsonObject status, int totalSlots) {
        if (status == null || !status.has("offers") || !status.get("offers").isJsonArray()) {
            return totalSlots;
        }
        int used = 0;
        for (JsonElement offerElement : status.getAsJsonArray("offers")) {
            if (!offerElement.isJsonObject()) {
                continue;
            }
            JsonObject offer = offerElement.getAsJsonObject();
            int itemId = readInt(offer, "item_id", 0);
            if (itemId != 0) {
                used++;
            }
        }
        return Math.max(0, totalSlots - used);
    }

    private long inferAvailableCoins(JsonObject status) {
        if (status == null || !status.has("items") || !status.get("items").isJsonArray()) {
            return 0;
        }
        long availableCoins = 0;
        for (JsonElement itemElement : status.getAsJsonArray("items")) {
            if (!itemElement.isJsonObject()) {
                continue;
            }
            JsonObject item = itemElement.getAsJsonObject();
            if (readInt(item, "item_id", -1) == 995) {
                availableCoins += readLong(item, "amount", 0);
            }
        }
        return availableCoins;
    }

    private Set<Integer> inferBlockedItems(JsonObject status) {
        Set<Integer> blockedItems = new HashSet<>();
        if (status == null || !status.has("blocked_items") || !status.get("blocked_items").isJsonArray()) {
            return blockedItems;
        }
        for (JsonElement e : status.getAsJsonArray("blocked_items")) {
            if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isNumber()) {
                blockedItems.add(e.getAsInt());
            }
        }
        return blockedItems;
    }

    private int readInt(JsonObject object, String key, int defaultValue) {
        if (object.has(key) && object.get(key).isJsonPrimitive() && object.get(key).getAsJsonPrimitive().isNumber()) {
            return object.get(key).getAsInt();
        }
        return defaultValue;
    }

    private long readLong(JsonObject object, String key, long defaultValue) {
        if (object.has(key) && object.get(key).isJsonPrimitive() && object.get(key).getAsJsonPrimitive().isNumber()) {
            return object.get(key).getAsLong();
        }
        return defaultValue;
    }

    private Double readDouble(JsonObject object, String key, Double defaultValue) {
        if (object.has(key) && object.get(key).isJsonPrimitive() && object.get(key).getAsJsonPrimitive().isNumber()) {
            return object.get(key).getAsDouble();
        }
        return defaultValue;
    }

    private boolean readBoolean(JsonObject object, String key, boolean defaultValue) {
        if (object.has(key) && object.get(key).isJsonPrimitive() && object.get(key).getAsJsonPrimitive().isBoolean()) {
            return object.get(key).getAsBoolean();
        }
        return defaultValue;
    }

    private String readString(JsonObject object, String key, String defaultValue) {
        if (object.has(key) && object.get(key).isJsonPrimitive() && object.get(key).getAsJsonPrimitive().isString()) {
            return object.get(key).getAsString();
        }
        return defaultValue;
    }

    private int resolveContentLength(Response resp) throws IOException {
        try {
            String cl = resp.header("Content-Length");
            return Integer.parseInt(cl != null ? cl : "missing Content-Length header");
        } catch (NumberFormatException  e) {
            throw new IOException("Failed to parse response Content-Length", e);
        }
    }

    private int resolveSuggestionContentLength(Response resp) throws IOException {
        try {
            String cl = resp.header("X-Suggestion-Content-Length");
            return Integer.parseInt(cl != null ? cl : "missing Content-Length header");
        } catch (NumberFormatException  e) {
            throw new IOException("Failed to parse response Content-Length", e);
        }
    }

    public void sendTransactionsAsync(List<Transaction> transactions, String displayName, BiConsumer<Integer, List<FlipV2>> onSuccess, Consumer<HttpResponseException> onFailure) {
        log.debug("sending {} transactions for display name {}", transactions.size(), displayName);
        JsonArray body = new JsonArray();
        for (Transaction transaction : transactions) {
            body.add(transaction.toJsonObject());
        }
        Integer userId = copilotLoginRS.get().getUserId();
        String jwtToken = copilotLoginRS.get().getJwtToken();
        String encodedDisplayName = URLEncoder.encode(displayName, StandardCharsets.UTF_8);
        Request request = new Request.Builder()
                .url(serverUrl + "/profit-tracking/client-transactions?display_name=" + encodedDisplayName)
                .addHeader("Authorization", "Bearer " + jwtToken)
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), body.toString()))
                .header("Accept", "application/x-bytes")
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.warn("call to sync transactions failed", e);
                onFailure.accept(new HttpResponseException(-1, UNKNOWN_ERROR));
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        String errorMessage = extractErrorMessage(response);
                        log.warn("call to sync transactions failed status code {}, error message {}", response.code(), errorMessage);
                        onFailure.accept(new HttpResponseException(response.code(), errorMessage));
                        return;
                    }
                    List<FlipV2> changedFlips = FlipV2.listFromRaw(response.body().bytes());
                    onSuccess.accept(userId, changedFlips);
                } catch (Exception e) {
                    log.warn("error reading/parsing sync transactions response body", e);
                    onFailure.accept(new HttpResponseException(-1, UNKNOWN_ERROR));
                }
            }
        });
    }

    private String extractErrorMessage(Response response) {
        if (response.body() != null) {
            try {
                String bodyStr = response.body().string();
                if (bodyStr == null || bodyStr.trim().isEmpty()) {
                    return UNKNOWN_ERROR;
                }

                JsonElement parsed = new JsonParser().parse(bodyStr);
                if (parsed != null && parsed.isJsonObject()) {
                    JsonObject errorJson = parsed.getAsJsonObject();
                    if (errorJson.has("message") && errorJson.get("message").isJsonPrimitive()) {
                        return errorJson.get("message").getAsString();
                    }
                }
                if (parsed != null && parsed.isJsonPrimitive()) {
                    JsonPrimitive primitive = parsed.getAsJsonPrimitive();
                    if (primitive.isString()) {
                        return primitive.getAsString();
                    }
                }
                return bodyStr;
            } catch (Exception e) {
                log.warn("failed reading/parsing error message from http {} response body", response.code(), e);
            }
        }
        return UNKNOWN_ERROR;
    }


    public void asyncGetVisualizeFlipData(UUID flipID, String displayName, Consumer<VisualizeFlipResponse> onSuccess, Consumer<String> onFailure) {
        JsonObject body = new JsonObject();
        body.add("flip_id", new JsonPrimitive(flipID.toString()));
        body.add("display_name", new JsonPrimitive(displayName));
        log.debug("requesting visualize data for flip {}", flipID);
        String jwtToken = copilotLoginRS.get().getJwtToken();
        Request request = new Request.Builder()
                .url(serverUrl +"/profit-tracking/visualize-flip")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .addHeader("Accept", "application/x-msgpack")
                .addHeader("X-VERSION", "1")
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), body.toString()))
                .build();

        client.newBuilder()
                .callTimeout(30, TimeUnit.SECONDS) // Overall timeout
                .build()
                .newCall(request)
                .enqueue(new Callback() {
                    @Override
                    public void onFailure(Call call, IOException e) {
                        onFailure.accept(e.toString());
                    }
                    @Override
                    public void onResponse(Call call, Response response) {
                        try {
                            if (!response.isSuccessful()) {
                                if(response.code() == UNAUTHORIZED_CODE && Objects.equals(jwtToken, copilotLoginRS.get().getJwtToken())) {
                                    copilotLoginRS.clear();
                                }
                                log.error("get visualize data for flip {} failed with http status code {}", flipID, response.code());
                                onFailure.accept(UNKNOWN_ERROR);
                            } else {
                                byte[] d = response.body().bytes();
                                VisualizeFlipResponse rsp = VisualizeFlipResponse.fromMsgPack(ByteBuffer.wrap(d));
                                log.debug("visualize data received for flip {}", flipID);
                                onSuccess.accept(rsp);
                            }
                        } catch (Exception e) {
                            log.error("error visualize data received for flip {}", flipID, e);
                            onFailure.accept(UNKNOWN_ERROR);
                        }
                    }
                });
    }

    public void asyncGetItemPriceWithGraphData(int itemId, String displayName, Consumer<ItemPrice> consumer, boolean includeGraphData) {
        JsonObject body = new JsonObject();
        body.add("item_id", new JsonPrimitive(itemId));
        body.add("display_name", new JsonPrimitive(displayName));
        body.addProperty("f2p_only", preferencesManager.isF2pOnlyMode());
        body.addProperty("timeframe_minutes", preferencesManager.getTimeframe());
        body.addProperty("risk_level", preferencesManager.getRiskLevel().toApiValue());
        body.addProperty("include_graph_data", includeGraphData);
        log.debug("requesting price graph data for item {}", itemId);
        String jwtToken = copilotLoginRS.get().getJwtToken();
        Request request = new Request.Builder()
                .url(serverUrl +"/prices")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .addHeader("Accept", "application/x-msgpack")
                .addHeader("X-VERSION", "1")
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), body.toString()))
                .build();

        client.newBuilder()
                .callTimeout(30, TimeUnit.SECONDS) // Overall timeout
                .build()
                .newCall(request)
                .enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("error fetching copilot price for item {}", itemId, e);
                ItemPrice ip = new ItemPrice(0, 0, DEFAULT_COPILOT_PRICE_ERROR_MESSAGE, null);
                clientThread.invoke(() -> consumer.accept(ip));
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        log.error("get copilot price for item {} failed with http status code {}", itemId, response.code());
                        ItemPrice ip = new ItemPrice(0, 0, DEFAULT_COPILOT_PRICE_ERROR_MESSAGE, null);
                        clientThread.invoke(() -> consumer.accept(ip));
                    } else {
                        byte[] d = response.body().bytes();
                        ItemPrice ip = ItemPrice.fromMsgPack(ByteBuffer.wrap(d));
                        log.debug("price graph data received for item {}", itemId);
                        clientThread.invoke(() -> consumer.accept(ip));
                    }
                } catch (Exception e) {
                    log.error("error fetching copilot price for item {}", itemId, e);
                    ItemPrice ip = new ItemPrice(0, 0, DEFAULT_COPILOT_PRICE_ERROR_MESSAGE, null);
                    clientThread.invoke(() -> consumer.accept(ip));
                }
            }
        });
    }


    public void asyncUpdatePremiumInstances(Consumer<PremiumInstanceStatus> consumer, List<String> displayNames) {
        JsonObject payload = new JsonObject();
        JsonArray arr = new JsonArray();
        displayNames.forEach(arr::add);
        payload.add("premium_display_names", arr);
        String jwtToken = copilotLoginRS.get().getJwtToken();

        Request request = new Request.Builder()
                .url(serverUrl +"/premium-instances/update-assignments")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), payload.toString()))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("error updating premium instance assignments", e);
                clientThread.invoke(() -> consumer.accept(PremiumInstanceStatus.ErrorInstance(DEFAULT_PREMIUM_INSTANCE_ERROR_MESSAGE)));
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        log.error("update premium instances failed with http status code {}", response.code());
                        clientThread.invoke(() -> consumer.accept(PremiumInstanceStatus.ErrorInstance(DEFAULT_PREMIUM_INSTANCE_ERROR_MESSAGE)));
                    } else {
                        PremiumInstanceStatus ip = gson.fromJson(response.body().string(), PremiumInstanceStatus.class);
                        clientThread.invoke(() -> consumer.accept(ip));
                    }
                } catch (Exception e) {
                    log.error("error updating premium instance assignments", e);
                    clientThread.invoke(() -> consumer.accept(PremiumInstanceStatus.ErrorInstance(DEFAULT_PREMIUM_INSTANCE_ERROR_MESSAGE)));
                }
            }
        });
    }

    public void asyncGetPremiumInstanceStatus(Consumer<PremiumInstanceStatus> consumer) {
        String jwtToken = copilotLoginRS.get().getJwtToken();
        Request request = new Request.Builder()
                .url(serverUrl +"/premium-instances/status")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .get()
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("error fetching premium instance status", e);
                clientThread.invoke(() -> consumer.accept(PremiumInstanceStatus.ErrorInstance(DEFAULT_PREMIUM_INSTANCE_ERROR_MESSAGE)));
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        log.error("get premium instance status failed with http status code {}", response.code());
                        clientThread.invoke(() -> consumer.accept(PremiumInstanceStatus.ErrorInstance(DEFAULT_PREMIUM_INSTANCE_ERROR_MESSAGE)));
                    } else {
                        PremiumInstanceStatus ip = gson.fromJson(response.body().string(), PremiumInstanceStatus.class);
                        clientThread.invoke(() -> consumer.accept(ip));
                    }
                } catch (Exception e) {
                    log.error("error fetching premium instance status", e);
                    clientThread.invoke(() -> consumer.accept(PremiumInstanceStatus.ErrorInstance(DEFAULT_PREMIUM_INSTANCE_ERROR_MESSAGE)));
                }
            }
        });

    }

    public void asyncDeleteFlip(FlipV2 flip, Consumer<FlipV2> onSuccess, Runnable onFailure) {
        JsonObject body = new JsonObject();
        body.addProperty("flip_id", flip.getId().toString());
        String jwtToken = copilotLoginRS.get().getJwtToken();

        Request request = new Request.Builder()
                .url(serverUrl + "/profit-tracking/delete-flip")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .header("Accept", "application/x-bytes")
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), body.toString()))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("deleting flip {}", flip.getId(), e);
                onFailure.run();
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        log.error("deleting flip {}, bad response code {}", flip.getId(), response.code());
                        onFailure.run();
                    } else {
                        FlipV2 flip = FlipV2.fromRaw(response.body().bytes());
                        onSuccess.accept(flip);
                    }
                } catch (Exception e) {
                    log.error("deleting flip {}", flip.getId(), e);
                    onFailure.run();
               }
            }
        });
    }

    public void asyncDeleteAccount(int accountId, Runnable onSuccess, Runnable onFailure) {
        JsonObject body = new JsonObject();
        body.addProperty("account_id", accountId);
        String jwtToken = copilotLoginRS.get().getJwtToken();

        Request request = new Request.Builder()
                .url(serverUrl + "/profit-tracking/delete-account")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .header("Accept", "application/x-bytes")
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), body.toString()))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("deleting account {}", accountId, e);
                onFailure.run();
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        log.error("deleting account {}, bad response code {}", accountId, response.code());
                        onFailure.run();
                    }
                    onSuccess.run();
                } catch (Exception e) {
                    log.error("deleting account {}", accountId, e);
                    onFailure.run();
                }
            }
        });
    }

    public void asyncLoadAccounts(Consumer<Map<String, Integer>> onSuccess, Consumer<String> onFailure) {
        String jwtToken = copilotLoginRS.get().getJwtToken();
        Request request = new Request.Builder()
                .url(serverUrl + "/profit-tracking/rs-account-names")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .method("GET", null)
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("error loading user display names", e);
                onFailure.accept(UNKNOWN_ERROR);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        String errorMessage = extractErrorMessage(response);
                        log.error("load user display names failed with http status code {}, error message {}", response.code(), errorMessage);
                        onFailure.accept(errorMessage);
                        return;
                    }
                    String responseBody = response.body() != null ? response.body().string() : "{}";
                    Type respType = new TypeToken<Map<String, Integer>>(){}.getType();
                    Map<String, Integer> names = gson.fromJson(responseBody, respType);
                    Map<String, Integer> result = names != null ? names : new HashMap<>();
                    onSuccess.accept(result);
                } catch (Exception e) {
                    log.error("error reading/parsing user display names response body", e);
                    onFailure.accept(UNKNOWN_ERROR);
                }
            }
        });
    }

    public void asyncLoadFlips(Map<Integer, Integer> accountIdTime, BiConsumer<Integer, FlipsDeltaResult> onSuccess, Consumer<String> onFailure) {
        Integer userId = copilotLoginRS.get().getUserId();
        String jwtToken = copilotLoginRS.get().getJwtToken();
        DataDeltaRequest body = new DataDeltaRequest(accountIdTime);
        String bodyStr = gson.toJson(body);

        Request request = new Request.Builder()
                .url(serverUrl + "/profit-tracking/client-flips-delta")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .header("Accept", "application/x-bytes")
                .method("POST", RequestBody.create(MediaType.get("application/json; charset=utf-8"), bodyStr))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("error loading flips", e);
                onFailure.accept(UNKNOWN_ERROR);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        String errorMessage = extractErrorMessage(response);
                        log.error("load flips failed with http status code {}, error message {}", response.code(), errorMessage);
                        onFailure.accept(errorMessage);
                        return;
                    }
                    FlipsDeltaResult res = FlipsDeltaResult.fromRaw(response.body().bytes());
                    onSuccess.accept(userId, res);
                } catch (Exception e) {
                    log.error("error reading/parsing flips response body", e);
                    onFailure.accept(UNKNOWN_ERROR);
                }
            }
        });
    }

    public void asyncLoadTransactionsData(Consumer<byte[]> onSuccess, Consumer<String> onFailure) {
        String jwtToken = copilotLoginRS.get().getJwtToken();

        Request request = new Request.Builder()
                .url(serverUrl + "/profit-tracking/client-transactions")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .header("Accept", "application/x-bytes")
                .get()
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("error loading transactions", e);
                onFailure.accept(UNKNOWN_ERROR);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        String errorMessage = extractErrorMessage(response);
                        log.error("load transactions failed with http status code {}, error message {}", response.code(), errorMessage);
                        onFailure.accept(errorMessage);
                        return;
                    }
                    byte[] data = response.body().bytes();
                    onSuccess.accept(Arrays.copyOfRange(data, 4, data.length-4));
                } catch (Exception e) {
                    log.error("error reading/parsing transactions response body", e);
                    onFailure.accept(UNKNOWN_ERROR);
                }
            }
        });
    }

    public Call asyncConsumeDumpAlerts(String displayName, Consumer<Response> onSuccess, Consumer<HttpResponseException> onFailure) {
        String encodedDisplayName = URLEncoder.encode(displayName, StandardCharsets.UTF_8);
        String jwtToken = copilotLoginRS.get().getJwtToken();
        Request request = new Request.Builder()
                .url(serverUrl + "/dump-alerts?display_name=" + encodedDisplayName)
                .addHeader("Authorization", "Bearer " + jwtToken)
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), ""))
                .build();

        Call call = client.newBuilder()
                .readTimeout(10, TimeUnit.SECONDS)
                .callTimeout(0, TimeUnit.MILLISECONDS)
                .build()
                .newCall(request);

        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.warn("error consuming dump alerts", e);
                onFailure.accept(new HttpResponseException(-1, UNKNOWN_ERROR));
            }

            @Override
            public void onResponse(Call call, Response response) {
                if (!response.isSuccessful()) {
                    if(response.code() == UNAUTHORIZED_CODE && Objects.equals(jwtToken, copilotLoginRS.get().getJwtToken())) {
                        copilotLoginRS.clear();
                    }
                    String errorMessage = extractErrorMessage(response);
                    response.close();
                    onFailure.accept(new HttpResponseException(response.code(), errorMessage));
                    return;
                }
                if (response.body() == null) {
                    response.close();
                    onFailure.accept(new HttpResponseException(-1, UNKNOWN_ERROR));
                    return;
                }
                onSuccess.accept(response);
            }
        });

        return call;
    }


    public void asyncOrphanTransaction(AckedTransaction transaction, BiConsumer<Integer, List<FlipV2>> onSuccess, Runnable onFailure) {
        JsonObject body = new JsonObject();
        body.addProperty("transaction_id", transaction.getId().toString());
        body.addProperty("account_id", transaction.getAccountId());
        Integer userId = copilotLoginRS.get().getUserId();
        String jwtToken = copilotLoginRS.get().getJwtToken();
        Request request = new Request.Builder()
                .url(serverUrl + "/profit-tracking/orphan-transaction")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .header("Accept", "application/x-bytes")
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), body.toString()))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("orphaning transaction {}", transaction.getId(), e);
                onFailure.run();
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        log.error("orphaning transaction {}, bad response code {}", transaction.getId(), response.code());
                        onFailure.run();
                    } else {
                        List<FlipV2> flips = FlipV2.listFromRaw(response.body().bytes());
                        onSuccess.accept(userId, flips);
                    }
                } catch (Exception e) {
                    log.error("orphaning transaction {}", transaction.getId(), e);
                    onFailure.run();
                }
            }
        });
    }

    public void asyncDeleteTransaction(AckedTransaction transaction, BiConsumer<Integer, List<FlipV2>> onSuccess, Runnable onFailure) {
        JsonObject body = new JsonObject();
        body.addProperty("transaction_id", transaction.getId().toString());
        body.addProperty("account_id", transaction.getAccountId());
        Integer userId = copilotLoginRS.get().getUserId();
        String jwtToken = copilotLoginRS.get().getJwtToken();
        Request request = new Request.Builder()
                .url(serverUrl + "/profit-tracking/delete-transaction")
                .addHeader("Authorization", "Bearer " + jwtToken)
                .header("Accept", "application/x-bytes")
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), body.toString()))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("delete transaction {}", transaction.getId(), e);
                onFailure.run();
            }
            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        log.error("delete transaction {}, bad response code {}", transaction.getId(), response.code());
                        onFailure.run();
                    } else {
                        List<FlipV2> flips = FlipV2.listFromRaw(response.body().bytes());
                        onSuccess.accept(userId, flips);
                    }
                } catch (Exception e) {
                    log.error("delete transaction {}", transaction.getId(), e);
                    onFailure.run();
                }
            }
        });
    }

    public void asyncLoadRecentAccountTransactions(String displayName, int endTime, Consumer<List<AckedTransaction>> onSuccess, Consumer<String> onFailure) {
        JsonObject body = new JsonObject();
        body.addProperty("limit", 30);
        body.addProperty("end", endTime);
        String jwtToken = copilotLoginRS.get().getJwtToken();
        Request request = new Request.Builder()
                .url(serverUrl + "/profit-tracking/account-client-transactions?display_name=" + displayName)
                .addHeader("Authorization", "Bearer " + jwtToken)
                .header("Accept", "application/x-bytes")
                .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), body.toString()))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("error loading transactions", e);
                onFailure.accept(UNKNOWN_ERROR);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        String errorMessage = extractErrorMessage(response);
                        log.error("load transactions failed with http status code {}, error message {}", response.code(), errorMessage);
                        onFailure.accept(errorMessage);
                        return;
                    }
                    onSuccess.accept(AckedTransaction.listFromRaw(response.body().bytes()));
                } catch (Exception e) {
                    log.error("error reading/parsing transactions response body", e);
                    onFailure.accept(UNKNOWN_ERROR);
                }
            }
        });
    }
}
