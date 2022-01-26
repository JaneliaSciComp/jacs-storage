package org.janelia.jacsstorage.expr;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.el.ELContext;
import javax.el.ELManager;
import javax.el.ELProcessor;
import javax.el.ExpressionFactory;
import javax.el.PropertyNotFoundException;
import javax.el.ValueExpression;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExprHelper {

    static abstract class AbstractNode {
        private final String val;

        AbstractNode(String val) {
            this.val = val;
        }

        String getVal() {
            return val;
        }
    }

    static class ConstNode extends AbstractNode {
        ConstNode(String val) {
            super(val);
        }

        public String toString() {
            return getVal();
        }
    }

    static class VarNode extends AbstractNode {
        VarNode(String varName) {
            super(varName);
        }

        public String toString() {
            return "${" + getVal() + "}";
        }
    }

    static class ValExpr {
        private final List<AbstractNode> exprNodes = new ArrayList<>();

        void addConst(String constExpr) {
            if (StringUtils.isNotEmpty(constExpr)) {
                exprNodes.add(new ConstNode(constExpr));
            }
        }

        void addVar(String varName) {
            Preconditions.checkArgument(StringUtils.isNotBlank(varName));
            exprNodes.add(new VarNode(varName));
        }

        @Override
        public String toString() {
            return exprNodes.toString();
        }
    }

    public static String eval(String argExpr, Map<String, Object> evalContext) {
        ELProcessor elp = new ELProcessor();
        ELManager elm = elp.getELManager();
        ExpressionFactory factory = ELManager.getExpressionFactory();
        evalContext.forEach((field, value) -> {
            elm.setVariable(field, factory.createValueExpression(value, Object.class));
        });
        ELContext context = elm.getELContext();
        ValueExpression argValExpr = factory.createValueExpression(context, argExpr, Object.class);
        try {
            Object argValue = argValExpr.getValue(context);
            if (argValue == null) {
                return argExpr;
            } else {
                return argValue.toString();
            }
        } catch (PropertyNotFoundException e) {
            return argExpr;
        }
    }

    public static MatchingResult match(String strExpr, String value) {
        if (strExpr == null || value == null) {
            return new MatchingResult(false,
                    "Cannot match null expr or null value; " +
                            "expr=" + strExpr  + "," +
                            "val=" + value,
                    "",
                    value
            );
        } else {
            return matchValExp(parse(strExpr), value);
        }

    }

    public static String getConstPrefix(String strExpr) {
        ValExpr valExpr = parse(strExpr);
        if (valExpr.exprNodes.isEmpty() || valExpr.exprNodes.get(0) instanceof VarNode) {
            return "";
        } else {
            return valExpr.exprNodes.get(0).getVal();
        }
    }

    public static Set<String> extractVarNames(String strExpr) {
        Set<String> varNames = new HashSet<>();
        ValExpr valExpr = parse(strExpr);
        for (AbstractNode node : valExpr.exprNodes) {
            if (node instanceof VarNode) {
                varNames.add(node.getVal());
            }
        }
        return varNames;
    }

    private static ValExpr parse(String expr) {
        ValExpr valExpr = new ValExpr();
        if (StringUtils.isNotBlank(expr)) {
            Pattern placeholderPattern = Pattern.compile("\\$\\{(.+?)\\}");
            Matcher m = placeholderPattern.matcher(expr);
            int exprIndex = 0;
            while (m.find(exprIndex)) {
                int startRegion = m.start();
                int endRegion = m.end();
                String varName = expr.substring(m.start() + 2, m.end() - 1);
                valExpr.addConst(expr.substring(exprIndex, startRegion));
                valExpr.addVar(varName);
                exprIndex = endRegion;
            }
            valExpr.addConst(expr.substring(exprIndex));
        }
        return valExpr;
    }

    private static MatchingResult matchValExp(ValExpr expr, String val) {
        int valIndex = 0;
        List<VarNode> varNodeStack = new LinkedList<>();
        Map<String, String> varAssignments = new LinkedHashMap<>();
        for (AbstractNode node : expr.exprNodes) {
            if (node instanceof VarNode) {
                // push the var node to the stack because we may not have the variable assigned yet
                // and we don't know what to look for
                varNodeStack.add(0, (VarNode) node);
            } else if (node instanceof ConstNode) {
                int matchPos = val.indexOf(node.getVal(), valIndex);
                if (matchPos == -1) {
                    return new MatchingResult(false,
                            "No matching found for '" +
                                    node.getVal() + "' starting at " + valIndex  +
                                    " while matching: " + val,
                            val.substring(0, valIndex),
                            val.substring(valIndex)
                    );
                }
                if (matchPos == valIndex) {
                    // match occurred at the beginning
                    // this can only happen if the var stack is empty
                    if (!varNodeStack.isEmpty()) {
                        String varName = varNodeStack.remove(0).getVal();
                        return new MatchingResult(false,
                                "No matching found for variable '" +
                                        varName + "' at " + valIndex  +
                                        " even though a match was found for " +
                                        "'" + node.getVal() + "'" +
                                        " while matching: " + val,
                                val.substring(0, valIndex),
                                val.substring(valIndex)
                        );
                    }
                } else {
                    if (varNodeStack.isEmpty()) {
                        // if the match was not from the beginning then there must have been a variable
                        // on the stack that should match the "prefix" of the remaining string
                        return new MatchingResult(false,
                                "Matching found for '" +
                                        node.getVal() + "' at " + matchPos  +
                                        " but no match found for region between [" +
                                        valIndex + " - " + matchPos + "]",
                                val.substring(0, valIndex),
                                val.substring(valIndex)
                        );
                    }
                    String varName = varNodeStack.remove(0).getVal();
                    String varValue = val.substring(valIndex, matchPos);
                    String existingVarValue = varAssignments.get(varName);
                    if (existingVarValue == null) {
                        varAssignments.put(varName, varValue);
                    } else if (!varValue.equals(existingVarValue)) {
                        return new MatchingResult(false,
                                "Conflict found for variable '" +
                                        varName + "' at " + matchPos + ". " + varName +
                                        " previous match was " + existingVarValue,
                                val.substring(0, valIndex),
                                val.substring(valIndex)
                        );
                    }
                }
                valIndex = matchPos + node.getVal().length();
            }
        }
        if (!varNodeStack.isEmpty()) {
            String varName = varNodeStack.remove(0).getVal();
            String varValue = val.substring(valIndex);
            if (varValue.length() == 0) {
                return new MatchingResult(false,
                        "No match found for '" +
                                varName + "' at the end, after " + valIndex,
                        val.substring(0, valIndex),
                        val.substring(valIndex)
                );
            } else {
                String existingVarValue = varAssignments.get(varName);
                if (existingVarValue != null) {
                    if (!varValue.startsWith(existingVarValue)) {
                        return new MatchingResult(false,
                                "Possible conflict found for variable '" +
                                        varName + "' at " + valIndex + ". " + varName +
                                        " previous match was " + existingVarValue,
                                val.substring(0, valIndex),
                                val.substring(valIndex)
                        );
                    }
                }
            }
        }
        return new MatchingResult(true,
                "",
                val.substring(0, valIndex),
                val.substring(valIndex)
        );
    }
}
