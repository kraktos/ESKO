/**
 * 
 */

package code.dws.dto;

import java.util.ArrayList;
import java.util.List;

import org.semanticweb.owlapi.model.OWLClass;

/**
 * Data object representing a tree structure where every node is a DBPedia class
 * with a set of attributes,
 * 
 * @author Arnab Dutta
 */
public class GenericTreeNode {

	/**
	 * name of the node, the DBPedia class
	 */
	private OWLClass nodeName;

	/**
	 * value of the node denoting instances for this class
	 */
	private double nodeValue;

	/**
	 * re computed value for the nodes coming from the children
	 */
	private double nodeUpScore;

	/**
	 * re computed value for the nodes coming from the parent
	 */
	private double nodeDownScore;

	/**
	 * collection of child nodes
	 */
	private List<GenericTreeNode> children;

	/**
	 * depth at which this node is placed in the tree
	 */
	private int nodeLevel;

	/**
	 * @param nodeName
	 */
	public GenericTreeNode(OWLClass nodeName) {
		this.nodeName = nodeName;
	}

	/**
	 * @param nodeName
	 * @param nodeValue
	 */
	public GenericTreeNode(OWLClass nodeName, double nodeValue) {
		this.nodeName = nodeName;
		this.nodeValue = nodeValue;
		this.children = new ArrayList<GenericTreeNode>();
	}

	/**
	 * @return the nodeName
	 */
	public OWLClass getNodeName() {
		return this.nodeName;
	}

	/**
	 * @return the nodeValue
	 */
	public double getNodeValue() {
		return this.nodeValue;
	}

	/**
	 * @return the nodePseudoValueFromChild
	 */
	public double getNodeUpScore() {
		return this.nodeUpScore;
	}

	/**
	 * @return the nodePseudoValueFromParent
	 */
	public double getNodeDownScore() {
		return this.nodeDownScore;
	}

	/**
	 * @return the nodeLevel
	 */
	public int getNodeLevel() {
		return this.nodeLevel;
	}

	/**
	 * @param nodeName
	 *            the nodeName to set
	 */
	public void setNodeName(OWLClass nodeName) {
		this.nodeName = nodeName;
	}

	/**
	 * @param nodeValue
	 *            the nodeValue to set
	 */
	public void setNodeValue(double nodeValue) {
		this.nodeValue = nodeValue;
	}

	/**
	 * @param nodePseudoValueFromChild
	 *            the nodePseudoValueFromChild to set
	 */
	public void setNodeUpScore(double nodeUpScore) {
		this.nodeUpScore = nodeUpScore;
	}

	/**
	 * @param nodePseudoValueFromParent
	 *            the nodePseudoValueFromParent to set
	 */
	public void setNodeDownScore(double nodeDownScore) {
		this.nodeDownScore = nodeDownScore;
	}

	/**
	 * @param children
	 *            the children to set
	 */
	public void setChildren(List<GenericTreeNode> children) {
		this.children = children;
	}

	/**
	 * @param nodeLevel
	 *            the nodeLevel to set
	 */
	public void setNodeLevel(int nodeLevel) {
		this.nodeLevel = nodeLevel;
	}

	/**
	 * @return the children
	 */
	public List<GenericTreeNode> getChildren() {
		if (this.children != null)
			return this.children;
		else
			return new ArrayList<GenericTreeNode>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("GenericTreeNode [");
		if (nodeName != null) {
			builder.append("nodeName=");
			builder.append(this.nodeName);
			builder.append(", ");
		}
		if (nodeValue >= 0) {
			builder.append("nodeValue=");
			builder.append(this.nodeValue);
			builder.append(", ");
		}
		if (nodeUpScore >= 0) {
			builder.append("nodeUpValue=");
			builder.append(this.nodeUpScore);
			builder.append(", ");
		}
		if (nodeDownScore >= 0) {
			builder.append("nodeDownValue=");
			builder.append(this.nodeDownScore);
		}

		builder.append("]");
		return builder.toString();
	}

	public void addChild(GenericTreeNode child) {
		this.children.add(child);
	}

	/**
	 * returns the number of children of this node
	 * 
	 * @return
	 */
	public int getNumberOfChildren() {
		return getChildren().size();
	}

}
