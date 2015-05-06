package net.xanxys.treebeard.processor;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xyx on 15/05/06.
 */
class ExtractLinksFn extends DoFn<TableRow, TableRow> {
    private static final long serialVersionUID = 0;
    private static final String redirectTag = "#REDIRECT";
    private Pattern linkRegex;
    private Aggregator<Long> countSpecialNamespace;
    private Aggregator<Long> countMisplacedRedirect;
    private Aggregator<Long> countMultiRedirect;

    @Override
    public void startBundle(Context c) throws Exception {
        super.startBundle(c);
        linkRegex = Pattern.compile("\\[\\[(\\S+)\\]\\]");

        countSpecialNamespace = c.createAggregator("special_namespace", new Sum.SumLongFn());
        countMisplacedRedirect = c.createAggregator("misplaced_redirect", new Sum.SumLongFn());
        countMultiRedirect = c.createAggregator("multi_redirect", new Sum.SumLongFn());
    }

    @Override
    public void processElement(ProcessContext c) {
        final String aid = (String) c.element().get("ArticleId");
        final String text = (String) c.element().get("Text");

        final boolean isRedirect = text.trim().startsWith(redirectTag);
        final boolean containRedirect = text.contains(redirectTag);

        // Ignore strange (not starting with #REDIRECT) redirect pages altogether.
        if (!isRedirect && containRedirect) {
            countMisplacedRedirect.addValue(1L);
            return;
        }

        // Extract links.
        final Matcher matcher = linkRegex.matcher(text);
        final ArrayList<TableRow> links = new ArrayList<>();
        while (matcher.find()) {
            final String linkText = matcher.group(1);
            final int indexBar = linkText.indexOf('|');

            // Extract a proper article title and an optional human-friendly label to it.
            // linkText is either "Title|Label" or "Title"
            String linkTitle;
            String linkLabel;
            if (indexBar >= 0) {
                linkTitle = linkText.substring(0, indexBar);
                linkLabel = linkText.substring(indexBar + 1, linkText.length());
            } else {
                linkTitle = linkText;
                linkLabel = linkText;
            }

            linkTitle = normalizeTitle(linkTitle);

            // Omit special namespaces. (e.g. "Wikipedia:BlahBlah", "User:Foo", etc.)
            if (linkTitle.indexOf(':') >= 0) {
                countSpecialNamespace.addValue(1L);
                continue;
            }
            links.add(new TableRow()
                    .set("article_id_src", aid)
                    .set("link_title", linkTitle)
                    .set("link_label", linkLabel)
                    .set("redirect", isRedirect));
        }

        // Reject ambiguous redirects.
        if (isRedirect && links.size() != 1) {
            countMultiRedirect.addValue(1L);
            return;
        }


        for (TableRow link : links) {
            c.output(link);
        }
    }

    /**
     * Normalize title and anchor to the canonical wikipedia article name.
     * c.f. https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(technical_restrictions)
     * This function will *not* remove pipes.
     */
    private String normalizeTitle(String linkTitle) {
        // Title can contain anchors, e.g. "Something#SomeSection".
        // Remove the sharp and the anchor.
        final int indexAnchor = linkTitle.indexOf('#');
        if (indexAnchor >= 0) {
            linkTitle = linkTitle.substring(0, indexAnchor);
        }

        // Always capitalize the first letter.
        if (linkTitle.length() > 0) {
            linkTitle =
                    linkTitle.substring(0, 1).toUpperCase() +
                            linkTitle.substring(1);
        }

        return linkTitle;
    }
}
