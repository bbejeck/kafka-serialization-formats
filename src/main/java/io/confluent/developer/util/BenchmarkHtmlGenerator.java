package io.confluent.developer.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BenchmarkHtmlGenerator {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#,##0.000");

    public static void main(String[] args) {
        try {
            String jsonFilePath = args.length > 0 ? args[0] : "benchmark-results.json";
            String outputHtmlPath = args.length > 1 ? args[1] : "benchmark-results.html";

            File jsonFile = new File(jsonFilePath);

            // Debug information
            System.out.println("Looking for file at: " + jsonFile.getAbsolutePath());
            System.out.println("File exists: " + jsonFile.exists());
            System.out.println("Is file: " + jsonFile.isFile());
            System.out.println("Can read: " + jsonFile.canRead());

            // Parse JSON
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(jsonFile);

            // Generate HTML
            String html = generateHtml(rootNode);

            // Write to file
            Files.write(Paths.get(outputHtmlPath), html.getBytes());
            System.out.println("✓ HTML report generated successfully: " + outputHtmlPath);

        } catch (IOException e) {
            System.err.println("Error generating HTML report: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String generateHtml(JsonNode rootNode) {
        StringBuilder html = new StringBuilder();

        // Parse benchmarks and separate by mode and operation type
        List<BenchmarkData> allBenchmarks = parseBenchmarks(rootNode);
        List<BenchmarkData> throughputSerialize = new ArrayList<>();
        List<BenchmarkData> throughputDeserialize = new ArrayList<>();
        List<BenchmarkData> avgTimeSerialize = new ArrayList<>();
        List<BenchmarkData> avgTimeDeserialize = new ArrayList<>();

        for (BenchmarkData benchmark : allBenchmarks) {
            boolean isSerialize = isSerializationBenchmark(benchmark.fullName);

            if (benchmark.mode.equalsIgnoreCase("thrpt")) {
                if (isSerialize) {
                    throughputSerialize.add(benchmark);
                } else {
                    throughputDeserialize.add(benchmark);
                }
            } else if (benchmark.mode.equalsIgnoreCase("avgt")) {
                if (isSerialize) {
                    avgTimeSerialize.add(benchmark);
                } else {
                    avgTimeDeserialize.add(benchmark);
                }
            }
        }

        // Sort each category
        sortBenchmarksByMode(throughputSerialize, true);      // higher is better
        sortBenchmarksByMode(throughputDeserialize, true);    // higher is better
        sortBenchmarksByMode(avgTimeSerialize, false);        // lower is better
        sortBenchmarksByMode(avgTimeDeserialize, false);      // lower is better

        int totalThroughput = throughputSerialize.size() + throughputDeserialize.size();
        int totalAvgTime = avgTimeSerialize.size() + avgTimeDeserialize.size();

        // NOW generate HTML with processed data
        html.append("<!DOCTYPE html>\n");
        html.append("<html lang=\"en\">\n");
        html.append("<head>\n");
        html.append("    <meta charset=\"UTF-8\">\n");
        html.append("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
        html.append("    <title>Benchmark Results Report</title>\n");
        html.append("    <script src=\"https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js\"></script>\n");
        html.append(getCssStyles());
        html.append(getJavaScript(throughputSerialize, throughputDeserialize, avgTimeSerialize, avgTimeDeserialize));
        html.append("</head>\n");
        html.append("<body>\n");
        html.append("    <div class=\"container\">\n");
        html.append("        <header>\n");
        html.append("            <h1>📊 Benchmark Results Report</h1>\n");
        html.append("            <p class=\"timestamp\">Generated: " + new Date() + "</p>\n");
        html.append("        </header>\n");

        // Summary statistics
        html.append(generateSummary(rootNode, totalThroughput, totalAvgTime));

        // Tabs section
        html.append("        <div class=\"tabs-container\">\n");
        html.append("            <div class=\"tabs\">\n");
        html.append("                <button class=\"tab-button active\" onclick=\"switchTab('throughput')\">⚡ Throughput Tables (").append(totalThroughput).append(")</button>\n");
        html.append("                <button class=\"tab-button\" onclick=\"switchTab('throughput-charts')\">📊 Throughput Charts</button>\n");
        html.append("                <button class=\"tab-button\" onclick=\"switchTab('avgtime')\">⏱️ Avg Time Tables (").append(totalAvgTime).append(")</button>\n");
        html.append("                <button class=\"tab-button\" onclick=\"switchTab('avgtime-charts')\">📈 Avg Time Charts</button>\n");
        html.append("            </div>\n");

        // Throughput tab content
        html.append("            <div id=\"throughput\" class=\"tab-content active\">\n");
        html.append(generateGroupedTableHtml(throughputSerialize, throughputDeserialize, "Throughput Results", "Higher is Better (Faster)"));
        html.append("            </div>\n");

        // Average Time tab content
        html.append("            <div id=\"avgtime\" class=\"tab-content\">\n");
        html.append(generateGroupedTableHtml(avgTimeSerialize, avgTimeDeserialize, "Average Time Results", "Lower is Better (Faster)"));
        html.append("            </div>\n");

        // Throughput Charts tab
        html.append("            <div id=\"throughput-charts\" class=\"tab-content\">\n");
        html.append("                <div class=\"chart-container\">\n");
        html.append("                    <h2>Throughput - Serialization</h2>\n");
        html.append("                    <p class=\"subtitle\">Higher is Better (ops/time)</p>\n");
        html.append("                    <canvas id=\"throughputSerChart\"></canvas>\n");
        html.append("                </div>\n");
        html.append("                <div class=\"chart-container\">\n");
        html.append("                    <h2>Throughput - Deserialization</h2>\n");
        html.append("                    <p class=\"subtitle\">Higher is Better (ops/time)</p>\n");
        html.append("                    <canvas id=\"throughputDeserChart\"></canvas>\n");
        html.append("                </div>\n");
        html.append("            </div>\n");

        // Average Time Charts tab
        html.append("            <div id=\"avgtime-charts\" class=\"tab-content\">\n");
        html.append("                <div class=\"chart-container\">\n");
        html.append("                    <h2>Average Time - Serialization</h2>\n");
        html.append("                    <p class=\"subtitle\">Lower is Better (time/op)</p>\n");
        html.append("                    <canvas id=\"avgtimeSerChart\"></canvas>\n");
        html.append("                </div>\n");
        html.append("                <div class=\"chart-container\">\n");
        html.append("                    <h2>Average Time - Deserialization</h2>\n");
        html.append("                    <p class=\"subtitle\">Lower is Better (time/op)</p>\n");
        html.append("                    <canvas id=\"avgtimeDeserChart\"></canvas>\n");
        html.append("                </div>\n");
        html.append("            </div>\n");

        html.append("        </div>\n");

        html.append("    </div>\n");
        html.append("</body>\n");
        html.append("</html>\n");

        return html.toString();
    }

    private static boolean isSerializationBenchmark(String benchmarkName) {
        String lowerName = benchmarkName.toLowerCase();

        // Check if it's a deserialization benchmark (check this first, more specific)
        if (lowerName.contains("deserialization")) {
            return false; // It's deserialization
        }

        // Check if it's a serialization benchmark
        if (lowerName.contains("serialization")) {
            return true; // It's serialization
        }

        // If neither pattern is found, try other patterns
        if (lowerName.contains("deserialize") ||
                lowerName.contains("deser") ||
                lowerName.contains("reader") ||
                lowerName.contains("decode") ||
                lowerName.contains("parse")) {
            return false; // It's deserialization
        }

        if (lowerName.contains("serialize") ||
                lowerName.contains("ser") ||
                lowerName.contains("writer") ||
                lowerName.contains("write") ||
                lowerName.contains("encode")) {
            return true; // It's serialization
        }

        // Default: if we can't tell, assume it's serialization
        return true;
    }

    private static String generateGroupedTableHtml(List<BenchmarkData> serializeBenchmarks,
                                                   List<BenchmarkData> deserializeBenchmarks,
                                                   String title, String subtitle) {
        StringBuilder html = new StringBuilder();

        // Serialization section
        html.append("                <div class=\"table-container\">\n");
        html.append("                    <h2>").append(title).append(" - Serialization</h2>\n");
        html.append("                    <p class=\"subtitle\">").append(subtitle).append("</p>\n");

        if (serializeBenchmarks.isEmpty()) {
            html.append("                    <div class=\"no-results\">No serialization benchmarks found</div>\n");
        } else {
            html.append(generateTableRows(serializeBenchmarks));
        }

        html.append("                </div>\n");

        // Deserialization section
        html.append("                <div class=\"table-container\">\n");
        html.append("                    <h2>").append(title).append(" - Deserialization</h2>\n");
        html.append("                    <p class=\"subtitle\">").append(subtitle).append("</p>\n");

        if (deserializeBenchmarks.isEmpty()) {
            html.append("                    <div class=\"no-results\">No deserialization benchmarks found</div>\n");
        } else {
            html.append(generateTableRows(deserializeBenchmarks));
        }

        html.append("                </div>\n");

        return html.toString();
    }

    private static String generateTableRows(List<BenchmarkData> benchmarks) {
        StringBuilder html = new StringBuilder();

        html.append("                    <table>\n");
        html.append("                        <thead>\n");
        html.append("                            <tr>\n");
        html.append("                                <th>Rank</th>\n");
        html.append("                                <th>Benchmark</th>\n");
        html.append("                                <th>Score</th>\n");
        html.append("                                <th>Error Margin</th>\n");
        html.append("                                <th>Unit</th>\n");
        html.append("                                <th>Samples</th>\n");
        html.append("                            </tr>\n");
        html.append("                        </thead>\n");
        html.append("                        <tbody>\n");

        int rank = 1;
        for (BenchmarkData benchmark : benchmarks) {
            html.append("                            <tr>\n");
            html.append("                                <td class=\"rank\">");
            if (rank == 1) {
                html.append("<span class=\"medal gold\">🥇</span>");
            } else if (rank == 2) {
                html.append("<span class=\"medal silver\">🥈</span>");
            } else if (rank == 3) {
                html.append("<span class=\"medal bronze\">🥉</span>");
            } else {
                html.append(rank);
            }
            html.append("</td>\n");
            html.append("                                <td class=\"benchmark-name\">");
            html.append("                                    <div class=\"name-main\">").append(escapeHtml(benchmark.shortName)).append("</div>\n");
            if (!benchmark.fullName.equals(benchmark.shortName)) {
                html.append("                                    <div class=\"name-full\">").append(escapeHtml(benchmark.fullName)).append("</div>\n");
            }
            html.append("                                </td>\n");
            html.append("                                <td class=\"score\">").append(DECIMAL_FORMAT.format(benchmark.score)).append("</td>\n");
            html.append("                                <td class=\"error\">± ").append(DECIMAL_FORMAT.format(benchmark.scoreError)).append("</td>\n");
            html.append("                                <td class=\"unit\">").append(escapeHtml(benchmark.scoreUnit)).append("</td>\n");
            html.append("                                <td class=\"samples\">").append(benchmark.samples).append("</td>\n");
            html.append("                            </tr>\n");
            rank++;
        }

        html.append("                        </tbody>\n");
        html.append("                    </table>\n");

        return html.toString();
    }

    private static List<BenchmarkData> parseBenchmarks(JsonNode rootNode) {
        List<BenchmarkData> benchmarks = new ArrayList<>();

        if (rootNode.isArray()) {
            for (JsonNode benchmark : rootNode) {
                BenchmarkData data = new BenchmarkData();
                data.fullName = benchmark.path("benchmark").asText();
                data.shortName = extractShortName(data.fullName);
                data.mode = benchmark.path("mode").asText();

                JsonNode primaryMetric = benchmark.path("primaryMetric");
                data.score = primaryMetric.path("score").asDouble();
                data.scoreError = primaryMetric.path("scoreError").asDouble();
                data.scoreUnit = primaryMetric.path("scoreUnit").asText();
                data.samples = !primaryMetric.path("scorePercentiles").isEmpty() ?
                        primaryMetric.path("scorePercentiles").size() :
                        benchmark.path("measurementIterations").asInt(0);

                benchmarks.add(data);
            }
        }

        return benchmarks;
    }

    private static String extractShortName(String fullName) {
        // Extract method name from full qualified name
        int lastDot = fullName.lastIndexOf('.');
        return lastDot >= 0 ? fullName.substring(lastDot + 1) : fullName;
    }

    private static void sortBenchmarksByMode(List<BenchmarkData> benchmarks, boolean higherIsBetter) {
        benchmarks.sort((b1, b2) -> {
            if (higherIsBetter) {
                // For throughput: higher is better - descending order
                return Double.compare(b2.score, b1.score);
            } else {
                // For time: lower is better - ascending order
                return Double.compare(b1.score, b2.score);
            }
        });
    }

    private static String generateSummary(JsonNode rootNode, int throughputCount, int avgTimeCount) {
        StringBuilder summary = new StringBuilder();

        int totalBenchmarks = rootNode.isArray() ? rootNode.size() : 0;
        Set<String> modes = new HashSet<>();

        if (rootNode.isArray()) {
            for (JsonNode benchmark : rootNode) {
                modes.add(benchmark.path("mode").asText());
            }
        }

        summary.append("        <div class=\"summary\">\n");
        summary.append("            <div class=\"summary-card\">\n");
        summary.append("                <div class=\"summary-value\">").append(totalBenchmarks).append("</div>\n");
        summary.append("                <div class=\"summary-label\">Total Benchmarks</div>\n");
        summary.append("            </div>\n");
        summary.append("            <div class=\"summary-card\">\n");
        summary.append("                <div class=\"summary-value\">").append(throughputCount).append("</div>\n");
        summary.append("                <div class=\"summary-label\">Throughput Tests</div>\n");
        summary.append("            </div>\n");
        summary.append("            <div class=\"summary-card\">\n");
        summary.append("                <div class=\"summary-value\">").append(avgTimeCount).append("</div>\n");
        summary.append("                <div class=\"summary-label\">Avg Time Tests</div>\n");
        summary.append("            </div>\n");
        summary.append("        </div>\n");

        return summary.toString();
    }

    private static String getCssStyles() {
        return """
            <style>
                * {
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }
                
                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: #333;
                    padding: 20px;
                    line-height: 1.6;
                }
                
                .container {
                    max-width: 1400px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 12px;
                    box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                    overflow: hidden;
                }
                
                header {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 40px;
                    text-align: center;
                }
                
                header h1 {
                    font-size: 2.5em;
                    margin-bottom: 10px;
                    font-weight: 700;
                }
                
                .timestamp {
                    opacity: 0.9;
                    font-size: 0.95em;
                }
                
                .summary {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    padding: 40px;
                    background: #f8f9fa;
                }
                
                .summary-card {
                    background: white;
                    padding: 30px;
                    border-radius: 10px;
                    text-align: center;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                    transition: transform 0.2s;
                }
                
                .summary-card:hover {
                    transform: translateY(-5px);
                    box-shadow: 0 8px 15px rgba(0, 0, 0, 0.15);
                }
                
                .summary-value {
                    font-size: 2.5em;
                    font-weight: bold;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    -webkit-background-clip: text;
                    -webkit-text-fill-color: transparent;
                    background-clip: text;
                }
                
                .summary-label {
                    color: #666;
                    font-size: 0.95em;
                    margin-top: 10px;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                }
                
                .tabs-container {
                    padding: 40px;
                }
                
                .tabs {
                    display: flex;
                    gap: 10px;
                    margin-bottom: 30px;
                    border-bottom: 2px solid #e0e0e0;
                }
                
                .tab-button {
                    background: none;
                    border: none;
                    padding: 15px 30px;
                    font-size: 1.1em;
                    font-weight: 600;
                    color: #666;
                    cursor: pointer;
                    position: relative;
                    transition: all 0.3s ease;
                    border-bottom: 3px solid transparent;
                }
                
                .tab-button:hover {
                    color: #667eea;
                    background: rgba(102, 126, 234, 0.05);
                }
                
                .tab-button.active {
                    color: #667eea;
                    border-bottom-color: #667eea;
                }
                
                .tab-content {
                    display: none;
                    animation: fadeIn 0.3s ease;
                }
                
                .tab-content.active {
                    display: block;
                }
                
                @keyframes fadeIn {
                    from {
                        opacity: 0;
                        transform: translateY(10px);
                    }
                    to {
                        opacity: 1;
                        transform: translateY(0);
                    }
                }
                
                .table-container {
                    margin-bottom: 40px;
                }
                
                .chart-container {
                    margin-bottom: 60px;
                    padding: 20px;
                    background: white;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
                }

                .chart-container canvas {
                    height: 500px !important;
                    max-height: 800px;
                }

                .no-results {
                    padding: 40px;
                    text-align: center;
                    color: #999;
                    font-style: italic;
                    background: #f9f9f9;
                    border-radius: 8px;
                    margin-bottom: 20px;
                }
                
                h2 {
                    color: #333;
                    margin-bottom: 10px;
                    font-size: 1.8em;
                    border-bottom: 3px solid #667eea;
                    padding-bottom: 10px;
                }
                
                .subtitle {
                    color: #666;
                    font-style: italic;
                    margin-bottom: 20px;
                    font-size: 1.05em;
                }
                
                table {
                    width: 100%;
                    border-collapse: collapse;
                    background: white;
                    border-radius: 8px;
                    overflow: hidden;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
                }
                
                thead {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                }
                
                th {
                    padding: 18px 15px;
                    text-align: left;
                    font-weight: 600;
                    text-transform: uppercase;
                    font-size: 0.85em;
                    letter-spacing: 1px;
                }
                
                td {
                    padding: 15px;
                    border-bottom: 1px solid #eee;
                }
                
                tbody tr {
                    transition: background-color 0.2s;
                }
                
                tbody tr:hover {
                    background-color: #f8f9ff;
                }
                
                tbody tr:last-child td {
                    border-bottom: none;
                }
                
                .benchmark-name {
                    font-weight: 500;
                }
                
                .name-main {
                    color: #2c3e50;
                    font-weight: 600;
                    font-size: 1.05em;
                }
                
                .name-full {
                    color: #7f8c8d;
                    font-size: 0.85em;
                    margin-top: 4px;
                    font-family: 'Courier New', monospace;
                }
                
                .badge {
                    display: inline-block;
                    padding: 6px 14px;
                    border-radius: 20px;
                    font-size: 0.85em;
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                }
                
                .badge-thrpt {
                    background: #d4edda;
                    color: #155724;
                }
                
                .badge-avgt {
                    background: #cce5ff;
                    color: #004085;
                }
                
                .badge-sample {
                    background: #fff3cd;
                    color: #856404;
                }
                
                .badge-ss {
                    background: #f8d7da;
                    color: #721c24;
                }
                
                .score {
                    font-weight: 600;
                    color: #27ae60;
                    font-size: 1.1em;
                    font-family: 'Courier New', monospace;
                }
                
                .error {
                    color: #e67e22;
                    font-family: 'Courier New', monospace;
                }
                
                .unit {
                    color: #7f8c8d;
                    font-style: italic;
                }
                
                .samples {
                    color: #95a5a6;
                    text-align: center;
                }
                
                .rank {
                    text-align: center;
                    font-weight: 600;
                    font-size: 1.1em;
                    color: #34495e;
                    width: 80px;
                }
                
                .medal {
                    font-size: 1.5em;
                }
                
                .gold { color: #FFD700; }
                .silver { color: #C0C0C0; }
                .bronze { color: #CD7F32; }
                
                @media (max-width: 768px) {
                    body {
                        padding: 10px;
                    }
                    
                    header h1 {
                        font-size: 1.8em;
                    }
                    
                    .summary {
                        grid-template-columns: 1fr;
                        padding: 20px;
                    }
                    
                    .tabs-container {
                        padding: 20px;
                    }
                    
                    .tabs {
                        flex-direction: column;
                    }
                    
                    .tab-button {
                        width: 100%;
                        text-align: left;
                    }
                    
                    .table-container {
                        overflow-x: auto;
                    }
                    
                    table {
                        font-size: 0.9em;
                    }
                    
                    th, td {
                        padding: 10px 8px;
                    }
                }
            </style>
            """;
    }

    private static String getJavaScript(List<BenchmarkData> throughputSer, 
                                        List<BenchmarkData> throughputDeser,
                                        List<BenchmarkData> avgtimeSer, 
                                        List<BenchmarkData> avgtimeDeser) {
        StringBuilder js = new StringBuilder();
        js.append("<script>\n");

        // Generate chart data
        js.append("const chartData = {\n");
        js.append("    throughputSer: ").append(generateChartData(throughputSer)).append(",\n");
        js.append("    throughputDeser: ").append(generateChartData(throughputDeser)).append(",\n");
        js.append("    avgtimeSer: ").append(generateChartData(avgtimeSer)).append(",\n");
        js.append("    avgtimeDeser: ").append(generateChartData(avgtimeDeser)).append("\n");
        js.append("};\n\n");

        // Tab switching function
        js.append("function switchTab(tabName) {\n");
        js.append("    const tabContents = document.querySelectorAll('.tab-content');\n");
        js.append("    tabContents.forEach(content => {\n");
        js.append("        content.classList.remove('active');\n");
        js.append("    });\n");
        js.append("    const tabButtons = document.querySelectorAll('.tab-button');\n");
        js.append("    tabButtons.forEach(button => {\n");
        js.append("        button.classList.remove('active');\n");
        js.append("    });\n");
        js.append("    document.getElementById(tabName).classList.add('active');\n");
        js.append("    event.target.classList.add('active');\n");
        js.append("}\n\n");

        // Chart creation function
        js.append("function createChart(canvasId, data, isHigherBetter) {\n");
        js.append("    const ctx = document.getElementById(canvasId).getContext('2d');\n");
        js.append("    return new Chart(ctx, {\n");
        js.append("        type: 'bar',\n");
        js.append("        data: {\n");
        js.append("            labels: data.labels,\n");
        js.append("            datasets: [{\n");
        js.append("                label: 'Score',\n");
        js.append("                data: data.scores,\n");
        js.append("                backgroundColor: 'rgba(102, 126, 234, 0.6)',\n");
        js.append("                borderColor: 'rgba(102, 126, 234, 1)',\n");
        js.append("                borderWidth: 2\n");
        js.append("            }]\n");
        js.append("        },\n");
        js.append("        options: {\n");
        js.append("            indexAxis: 'y',\n");
        js.append("            responsive: true,\n");
        js.append("            maintainAspectRatio: false,\n");
        js.append("            plugins: {\n");
        js.append("                legend: { display: false },\n");
        js.append("                tooltip: {\n");
        js.append("                    callbacks: {\n");
        js.append("                        label: function(context) {\n");
        js.append("                            return 'Score: ' + context.parsed.x.toFixed(3);\n");
        js.append("                        }\n");
        js.append("                    }\n");
        js.append("                }\n");
        js.append("            },\n");
        js.append("            scales: {\n");
        js.append("                x: {\n");
        js.append("                    beginAtZero: true,\n");
        js.append("                    title: { display: true, text: data.unit }\n");
        js.append("                },\n");
        js.append("                y: {\n");
        js.append("                    ticks: { autoSkip: false }\n");
        js.append("                }\n");
        js.append("            }\n");
        js.append("        }\n");
        js.append("    });\n");
        js.append("}\n\n");

        // Initialize charts on page load
        js.append("window.addEventListener('load', function() {\n");
        js.append("    createChart('throughputSerChart', chartData.throughputSer, true);\n");
        js.append("    createChart('throughputDeserChart', chartData.throughputDeser, true);\n");
        js.append("    createChart('avgtimeSerChart', chartData.avgtimeSer, false);\n");
        js.append("    createChart('avgtimeDeserChart', chartData.avgtimeDeser, false);\n");
        js.append("});\n");

        js.append("</script>\n");

        return js.toString();
    }

    private static String generateChartData(List<BenchmarkData> benchmarks) {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("        labels: [");

        for (int i = 0; i < benchmarks.size(); i++) {
            if (i > 0) json.append(", ");
            json.append("'").append(escapeHtml(benchmarks.get(i).shortName)).append("'");
        }

        json.append("],\n");
        json.append("        scores: [");

        for (int i = 0; i < benchmarks.size(); i++) {
            if (i > 0) json.append(", ");
            json.append(benchmarks.get(i).score);
        }

        json.append("],\n");
        json.append("        unit: '");
        if (!benchmarks.isEmpty()) {
            json.append(escapeHtml(benchmarks.get(0).scoreUnit));
        }
        json.append("'\n");
        json.append("    }");

        return json.toString();
    }

    private static String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }

    private static class BenchmarkData {
        String fullName;
        String shortName;
        String mode;
        double score;
        double scoreError;
        String scoreUnit;
        int samples;
    }
}