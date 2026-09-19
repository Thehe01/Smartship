package com.smartship.edge.benchmark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P1-4.1 Maven 配置隔离验证（正确性测试，随默认 {@code mvn test} 执行，不引入额外插件）。
 * <p>
 * 验证：默认 surefire 排除 {@code *BenchmarkTest}；{@code benchmark} profile 只纳入
 * {@code *BenchmarkTest} 并透传 benchmark 参数。
 */
@DisplayName("P1-4.1 Integrity: Maven benchmark isolation config")
class BenchmarkProfileConfigTest {

    @Test
    @DisplayName("pom 默认排除 benchmark，profile 只跑 benchmark")
    void pomIsolationConfig() throws Exception {
        assertTrue(Paths.get("pom.xml").toFile().exists(), "必须在模块根目录运行");
        Document doc = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder().parse(Paths.get("pom.xml").toFile());

        // 1. 默认 build 下 surefire 必须排除 *BenchmarkTest
        List<Element> basePlugins = childElements(
                child(child(doc.getDocumentElement(), "build"), "plugins"), "plugin");
        Element baseSurefire = findPlugin(basePlugins, "maven-surefire-plugin");
        assertNotNull(baseSurefire, "默认 build 必须声明 surefire");
        List<String> baseExcludes = texts(baseSurefire, "exclude");
        assertTrue(baseExcludes.stream().anyMatch(s -> s.contains("*BenchmarkTest")),
                "默认 surefire 必须排除 *BenchmarkTest，实际 excludes=" + baseExcludes);

        // 2. benchmark profile 必须存在且只纳入 *BenchmarkTest
        Element profile = null;
        for (Element e : childElements(child(doc.getDocumentElement(), "profiles"), "profile")) {
            if ("benchmark".equals(text(child(e, "id")))) {
                profile = e;
                break;
            }
        }
        assertNotNull(profile, "必须存在 id=benchmark 的 profile");
        List<Element> profPlugins = childElements(
                child(child(profile, "build"), "plugins"), "plugin");
        Element profSurefire = findPlugin(profPlugins, "maven-surefire-plugin");
        assertNotNull(profSurefire, "benchmark profile 必须声明 surefire");
        List<String> includes = texts(profSurefire, "include");
        assertTrue(includes.stream().anyMatch(s -> s.contains("*BenchmarkTest")),
                "benchmark profile 必须只纳入 *BenchmarkTest，实际 includes=" + includes);
        List<String> sysProps = childNames(child(
                child(profSurefire, "configuration"), "systemPropertyVariables"));
        assertTrue(sysProps.stream().anyMatch(s -> s.startsWith("benchmark.")),
                "benchmark profile 必须透传 benchmark.* 参数");
    }

    private static Element child(Element parent, String name) {
        if (parent == null) {
            return null;
        }
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            if (nodes.item(i) instanceof Element e && name.equals(e.getTagName())) {
                return e;
            }
        }
        return null;
    }

    private static List<Element> childElements(Element parent, String name) {
        List<Element> out = new ArrayList<>();
        if (parent == null) {
            return out;
        }
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            if (nodes.item(i) instanceof Element e && name.equals(e.getTagName())) {
                out.add(e);
            }
        }
        return out;
    }

    private static Element findPlugin(List<Element> plugins, String artifactId) {
        for (Element p : plugins) {
            Element a = child(p, "artifactId");
            if (a != null && artifactId.equals(a.getTextContent().trim())) {
                return p;
            }
        }
        return null;
    }

    private static List<String> texts(Element root, String tag) {
        List<String> out = new ArrayList<>();
        if (root == null) {
            return out;
        }
        NodeList nodes = root.getElementsByTagName(tag);
        for (int i = 0; i < nodes.getLength(); i++) {
            out.add(nodes.item(i).getTextContent().trim());
        }
        return out;
    }

    private static List<String> childNames(Element parent) {
        List<String> out = new ArrayList<>();
        if (parent == null) {
            return out;
        }
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            if (nodes.item(i) instanceof Element e) {
                out.add(e.getTagName());
            }
        }
        return out;
    }

    private static String text(Element e) {
        return e == null ? null : e.getTextContent().trim();
    }
}
