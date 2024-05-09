package io.github.dtolmachev1.inference.rule;

import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.data.table.Table;
import io.github.dtolmachev1.inference.analysis.Analysis;
import io.github.dtolmachev1.inference.analysis.AnalysisBuilderFactory;
import io.github.dtolmachev1.inference.analysis.TableNameAnalysis;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class TableNameInferenceRule implements InferenceRule {
    public static final String INFERENCE_RULE_NAME = "table-name";

    private TableNameInferenceRule() {
    }

    public static TableNameInferenceRule newInstance() {
        return TableNameInferenceRuleHolder.TABLE_NAME_INFERENCE_RULE;
    }

    @Override
    public String name() {
        return INFERENCE_RULE_NAME;
    }

    @Override
    public Optional<Analysis> apply(Database database) {
        TableNameAnalysis.TableNameAnalysisBuilder tableNameAnalysisBuilder = (TableNameAnalysis.TableNameAnalysisBuilder) AnalysisBuilderFactory.getAnalysisBuilder(INFERENCE_RULE_NAME);
        int tableNameCount = Math.toIntExact(database.stream()
                .map(entry -> determineTableName(entry.getValue(), tableNameAnalysisBuilder))
                .filter(value -> value)
                .count());
        return tableNameCount > 0 ? Optional.of(tableNameAnalysisBuilder.build()) : Optional.empty();
    }

    private boolean determineTableName(Table table, TableNameAnalysis.TableNameAnalysisBuilder tableNameAnalysisBuilder) {
        String tableNameCandidate = table.sources().stream()
                .map(TableNameInferenceRule::getFileName)
                .reduce(TableNameInferenceRule::longestCommonSubstring)
                .orElse(table.getName());
        if (!tableNameCandidate.equals(table.getName())) {
            tableNameAnalysisBuilder.tableName(table.getName(), tableNameCandidate);
            return true;
        }
        return false;
    }

    private static String getFileName(Path filePath) {
        String fileName = filePath.getFileName().toString();
        int index = fileName.lastIndexOf(".");
        return index != -1 ? fileName.substring(0, index) : fileName;
    }

    @SuppressWarnings({"DuplicateExpressions", "RedundantTypeArguments"})
    private static String longestCommonSubstring(String a, String b) {
        List<List<Integer>> dynamicTable = Stream.generate(() -> Stream.generate(() -> 0)
                .limit(b.length() + 1)
                .collect(ArrayList<Integer>::new, ArrayList::add, ArrayList::addAll))
                .limit(2)
                .collect(ArrayList<List<Integer>>::new, ArrayList::add, ArrayList::addAll);
        String result = "";
        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {
                if (a.charAt(i - 1) == b.charAt(j - 1) && (dynamicTable.get((i - 1) % 2).get(j - 1) != 0 || Character.isLetter(b.charAt(j - 1)))) {
                    dynamicTable.get(i % 2).set(j, dynamicTable.get((i - 1) % 2).get(j - 1) + 1);
                    if (dynamicTable.get(i % 2).get(j) > result.length() && Character.isLetter(b.charAt(j - 1))) {
                        result = b.substring(j - dynamicTable.get(i % 2).get(j), j);
                    }
                } else {
                    dynamicTable.get(i % 2).set(j, 0);
                }
            }
        }
        return result;
    }

    private static class TableNameInferenceRuleHolder {
        private static final TableNameInferenceRule TABLE_NAME_INFERENCE_RULE = new TableNameInferenceRule();
    }
}
