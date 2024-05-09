package io.github.dtolmachev1.inference.manager;

import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.inference.analysis.Analysis;
import io.github.dtolmachev1.inference.analysis.AnalysisFactory;
import io.github.dtolmachev1.inference.rule.InferenceRule;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class GenericInferenceManager implements InferenceManager {
    @SuppressWarnings("HttpUrlsUsage")
    private static final String XML_INDENT_AMOUNT_KEY = "{http://xml.apache.org/xslt}indent-amount";
    private static final String ANALYZES_TAG = "analyzes";
    private static final String ANALYSIS_TAG = "analysis";
    private static final String ANALYSIS_NAME_TAG = "analysis-name";
    private final Pipeline pipeline;
    private final Map<String, Analysis> analyzes;

    public GenericInferenceManager() {
        this(GenericPipeline.defaultPipeline());
    }

    GenericInferenceManager(Pipeline pipeline) {
        this.pipeline = pipeline;
        this.analyzes = new LinkedHashMap<>();
    }

    @Override
    public void loadAnalyzes(InputStream inputStream, Database database) {
        try {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
            load(document.getDocumentElement().getElementsByTagName(ANALYSIS_TAG));
            transform(database);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            throw new RuntimeException("Unable to load analyzes");
        }
    }

    @Override
    public void saveAnalyzes(OutputStream outputStream) {
        try {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            document.appendChild(save(document));
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(XML_INDENT_AMOUNT_KEY, "4");
            transformer.transform(new DOMSource(document), new StreamResult(outputStream));
        } catch (ParserConfigurationException | TransformerException e) {
            throw new RuntimeException("Unable to save analyzes");
        }
    }

    @Override
    public void analyzeDatabase(Database database) {
        for (InferenceRule inferenceRule : this.pipeline) {
            Optional<Analysis> analysis = inferenceRule.apply(database);
            if (analysis.isPresent()) {
                analysis.get().transform(database);
                this.analyzes.put(analysis.get().name(), analysis.get());
            }
        }
    }

    private void load(NodeList analyzesEntries) {
        if (analyzesEntries.getLength() == 0) {
            throw new RuntimeException("Unable to load analyzes");
        }
        for (int i = 0; i < analyzesEntries.getLength(); i++) {
            Element analysisEntries = (Element) analyzesEntries.item(i);
            String analysisName = analysisEntries.getElementsByTagName(ANALYSIS_NAME_TAG).item(0).getTextContent();
            Analysis analysis = AnalysisFactory.loadAnalysis(analysisName, analysisEntries.getElementsByTagName(analysisName));
            if (Objects.nonNull(analysis)) {
                this.analyzes.put(analysis.name(), analysis);
            }
        }
    }

    private Element save(Document document) {
        Element analyzesNode = document.createElement(ANALYZES_TAG);
        this.analyzes.values().forEach(analysis -> analyzesNode.appendChild(analysis.save(document)));
        return analyzesNode;
    }

    private void transform(Database database) {
        this.pipeline.stream()
                .map(InferenceRule::name)
                .filter(this.analyzes::containsKey)
                .map(this.analyzes::get)
                .forEach(analysis -> analysis.transform(database));
    }
}
