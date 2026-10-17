package com.festivalkafka.loadgen.scenario;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Resolve a {@link Scenario} from either (a) CLI flags, or (b) a YAML profile file
 * on the classpath / filesystem. CLI flags always win over file values so a
 * profile is just a convenient baseline a Makefile target can tweak.
 *
 * <p>Supported argument sources, in order of precedence (highest first):
 * <ol>
 *   <li>--scenario, --rate / --rps, --duration, --from-rps, --to-rps,
 *       --baseline-rps, --spike-multiplier, --spike-duration, --cooldown-duration,
 *       --hot-skus, --hot-sku-share</li>
 *   <li>Profile named by --profile from --scenario-file</li>
 * </ol>
 */
public final class ScenarioLoader {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private ScenarioLoader() {}

    public static Scenario resolve(Map<String, String> cli) {
        Map<String, Object> fromFile = readProfile(cli);

        String scenario = pickString(cli, fromFile, "scenario", List.of("--scenario"), "steady");

        return switch (scenario) {
            case "steady"         -> steady(cli, fromFile);
            case "ramp"           -> ramp(cli, fromFile);
            case "festival-spike" -> festivalSpike(cli, fromFile);
            case "lightning-deal" -> lightningDeal(cli, fromFile);
            default -> throw new IllegalArgumentException(
                    "unknown scenario: " + scenario + " (valid: steady|ramp|festival-spike|lightning-deal)");
        };
    }

    // ------------------------- scenario factories -------------------------

    private static Scenario steady(Map<String, String> cli, Map<String, Object> file) {
        int rps      = pickInt(cli, file, "rps",         List.of("--rate", "--rps"),              100);
        int duration = pickInt(cli, file, "durationSec", List.of("--duration", "--duration-sec"), 60);
        return new SteadyScenario(rps, duration);
    }

    private static Scenario ramp(Map<String, String> cli, Map<String, Object> file) {
        int fromRps  = pickInt(cli, file, "fromRps",     List.of("--from-rps"),                   100);
        int toRps    = pickInt(cli, file, "toRps",       List.of("--to-rps"),                     1000);
        int duration = pickInt(cli, file, "durationSec", List.of("--duration", "--duration-sec"), 300);
        return new RampScenario(fromRps, toRps, duration);
    }

    private static Scenario festivalSpike(Map<String, String> cli, Map<String, Object> file) {
        int baselineRps      = pickInt(cli,    file, "baselineRps",         List.of("--baseline-rps"),      100);
        double spikeMult     = pickDouble(cli, file, "spikeMultiplier",     List.of("--spike-multiplier"),  10.0);
        int baselineDuration = pickInt(cli,    file, "baselineDurationSec", List.of("--baseline-duration"), 60);
        int spikeDuration    = pickInt(cli,    file, "spikeDurationSec",    List.of("--spike-duration"),    60);
        int cooldown         = pickInt(cli,    file, "cooldownDurationSec", List.of("--cooldown-duration"), 30);
        return new FestivalSpikeScenario(baselineRps, spikeMult, baselineDuration, spikeDuration, cooldown);
    }

    private static Scenario lightningDeal(Map<String, String> cli, Map<String, Object> file) {
        int rps      = pickInt(cli,    file, "rps",         List.of("--rate", "--rps"),              200);
        int duration = pickInt(cli,    file, "durationSec", List.of("--duration", "--duration-sec"), 120);
        double share = pickDouble(cli, file, "hotShare",    List.of("--hot-sku-share"),              0.8);
        List<String> hotSkus = pickStringList(cli, file, "hotSkus", "--hot-skus",
                List.of("SKU-00001", "SKU-00002", "SKU-00003"));
        return new LightningDealScenario(rps, duration, hotSkus, share);
    }

    // ------------------------- YAML + value picking -------------------------

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readProfile(Map<String, String> cli) {
        String scenarioFile = cli.get("--scenario-file");
        String profile      = cli.get("--profile");
        if (profile == null) return Map.of();

        try (InputStream in = openScenarioFile(scenarioFile)) {
            Map<String, Object> root = YAML.readValue(in, Map.class);
            Map<String, Object> profiles = (Map<String, Object>) root.getOrDefault("profiles", Map.of());
            Map<String, Object> profileMap = (Map<String, Object>) profiles.get(profile);
            if (profileMap == null) {
                throw new IllegalArgumentException(
                        "profile '" + profile + "' not found in " +
                        Objects.requireNonNullElse(scenarioFile, "classpath:scenarios.yaml") +
                        " (available: " + profiles.keySet() + ")");
            }
            return profileMap;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read scenario file " + scenarioFile, e);
        }
    }

    private static InputStream openScenarioFile(String path) throws IOException {
        if (path == null || path.equals("classpath:scenarios.yaml")) {
            InputStream r = ScenarioLoader.class.getClassLoader().getResourceAsStream("scenarios.yaml");
            if (r == null) throw new IOException("classpath:scenarios.yaml not found on classpath");
            return r;
        }
        if (path.startsWith("classpath:")) {
            String rel = path.substring("classpath:".length());
            InputStream r = ScenarioLoader.class.getClassLoader().getResourceAsStream(rel);
            if (r == null) throw new IOException("classpath resource not found: " + rel);
            return r;
        }
        return Files.newInputStream(Path.of(path));
    }

    private static String pickString(Map<String, String> cli, Map<String, Object> file,
                                     String key, List<String> cliAliases, String fallback) {
        for (String a : cliAliases) {
            String v = cli.get(a);
            if (v != null) return v;
        }
        Object fv = file.get(key);
        return fv != null ? fv.toString() : fallback;
    }

    private static int pickInt(Map<String, String> cli, Map<String, Object> file,
                               String key, List<String> cliAliases, int fallback) {
        String v = pickString(cli, file, key, cliAliases, null);
        return v != null ? Integer.parseInt(v) : fallback;
    }

    private static double pickDouble(Map<String, String> cli, Map<String, Object> file,
                                     String key, List<String> cliAliases, double fallback) {
        String v = pickString(cli, file, key, cliAliases, null);
        return v != null ? Double.parseDouble(v) : fallback;
    }

    @SuppressWarnings("unchecked")
    private static List<String> pickStringList(Map<String, String> cli, Map<String, Object> file,
                                               String key, String cliFlag, List<String> fallback) {
        String csv = cli.get(cliFlag);
        if (csv != null && !csv.isBlank()) {
            return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
        }
        Object fv = file.get(key);
        if (fv instanceof List<?> l) {
            return l.stream().map(String::valueOf).toList();
        }
        return fallback;
    }
}
