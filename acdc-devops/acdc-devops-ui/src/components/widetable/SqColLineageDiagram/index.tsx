import createEngine, {
  DefaultLinkFactory,
  DefaultLinkModel,
  DefaultLinkSegmentWidget,
  DefaultLinkWidget,
  DefaultNodeModel,
  DefaultPortModel,
  DiagramEngine,
  DiagramModel,
} from '@projectstorm/react-diagrams';

import { Action, BaseModel, CanvasWidget, InputType } from '@projectstorm/react-canvas-core';
import './index.css';
import { PageLoading } from '@ant-design/pro-layout';
//import React, { memo, useEffect, useState } from 'react';
import React, { useEffect, useState } from 'react';

class GlobalSettings {
  // 节点颜色
  nodeColor: string;

  // 间距单位
  spacingUnit: number;

  // 画布距离左侧距离
  canvasLeftMargin: number;

  // 画布距离顶端距离
  canvasTopMargin: number;

  // x margin
  xMargin: number;

  // y margin
  yMargin: number;

  nodeMaxWidth: number;

  constructor() {
    this.nodeColor = 'rgb(0,192,255)';
    this.spacingUnit = 3;
    this.canvasLeftMargin = 20 * this.spacingUnit;
    this.canvasTopMargin = 50 * this.spacingUnit;
    this.xMargin = 150 * this.spacingUnit;
    this.yMargin = 60 * this.spacingUnit;
    this.nodeMaxWidth = 60 * this.spacingUnit;
  }
}
const defatulClobalSettings: GlobalSettings = new GlobalSettings();

export class SqColBeautifier {
  static getBeautifyExpression(expression: string): string {
    const matchResult: string[] | null = expression.match(/(?<=\.)\w+/gi);
    if (!matchResult || matchResult.length == 0) {
      return expression;
    }
    return matchResult[0];
  }

  static getBeautifyName(col: API.WideTableSubqueryColumn): string {
    const beautifyExpr = this.getBeautifyExpression(col.expression!);
    const leftLabel: string = col.alias ? col.alias : beautifyExpr!;
    return leftLabel;
  }

  static getBeautifyType(col: API.WideTableSubqueryColumn): string {
    const rightLabel: string = col.type ?? '';
    return rightLabel;
  }
}

// react-diagram link model and so on
export class HorveredLinkModel extends DefaultLinkModel {
  tree?: DiagramNodeTree;

  constructor(tree?: DiagramNodeTree) {
    super({
      type: 'hovered',
    });
    this.tree = tree;
  }

  public addPoint(_pointModel: any, _index: any): any {
    // 重写 addPoint 方法，使其不执行任何操作
  }
}

/**
 * Horver 事件.
 * */
export class HorveredLinkWidget extends DefaultLinkWidget {
  generateLink(path: string, extraProps: any, id: string | number): JSX.Element {
    const ref = React.createRef<SVGPathElement>();
    this.refPaths.push(ref);
    return (
      <DefaultLinkSegmentWidget
        key={`link-${id}`}
        path={path}
        selected={this.state.selected}
        diagramEngine={this.props.diagramEngine}
        factory={this.props.diagramEngine.getFactoryForLink(this.props.link)}
        link={this.props.link}
        forwardRef={ref}
        onSelection={(selected) => {
          let horveredLinkModel = this.props.link as HorveredLinkModel;
          horveredLinkModel.tree?.nodeItemOnSelection(
            this.props.link.getTargetPort().getOptions().extras,
            selected,
          );
          // this.setState({ selected: selected });
          this.props.diagramEngine.repaintCanvas();
        }}
        extras={extraProps}
      />
    );
  }
}

export class HorveredLinkFactory extends DefaultLinkFactory {
  constructor() {
    super('hovered');
  }

  generateModel(): any {
    return new HorveredLinkModel();
  }

  generateReactWidget(event: any) {
    return <HorveredLinkWidget link={event.model} diagramEngine={this.engine} />;
  }
}

/**
 * 鼠标点击事件.
 * */
class ClickAction extends Action {
  tree: DiagramNodeTree;

  constructor(tree: DiagramNodeTree) {
    super({
      type: InputType.MOUSE_UP,
      fire: (event: any) => {
        const element = this.engine.getActionEventBus().getModelForEvent(event);
        if (element) {
          if (element instanceof DefaultNodeModel) {
            tree.nodeOnClick(element.getOptions().extras);
          } else if (element instanceof HorveredLinkModel) {
            tree.nodeItemOnClick(element.getTargetPort().getOptions().extras);
          } else if (element instanceof DefaultPortModel) {
            // 只有 in 类型的 port 可以点击
            if (element.getOptions().alignment == 'left') {
              tree.nodeItemOnClick(element.getOptions().extras);
            }
          }
        } else {
          tree.unSelectAllLinks();
        }
        // console.debug(element + ' clicked')
      },
    });
    this.tree = tree;
  }
}

class NodeItem {
  // id
  id?: number;

  // 左侧端口标签
  leftLabel?: string;

  // 右侧端口标签
  rightLabel?: string;

  // 父节点 ID 集合
  parentIds?: number[];

  // in 端口
  inPort?: DefaultPortModel;

  // out 端口
  outPort?: DefaultPortModel;

  isSelected!: boolean;
}

class NodeLink {
  // out id
  outItemId: number;

  // in id
  inItemId: number;

  // link model
  link: HorveredLinkModel;

  constructor(outItemId: number, inItemId: number, link: HorveredLinkModel) {
    this.outItemId = outItemId;
    this.inItemId = inItemId;
    this.link = link;
  }
}

//class CustomNodeModel extends DefaultNodeModel {
//  constructor(options: DefaultNodeModelOptions, width: number) {
//    super(options);
//    this.width = width;
//  }
//}

//class CustomNodeWidget extends DefaultNodeWidget {
//  render() {
//    const { node } = this.props;
//    console.log('props:', this.props);
//    // 获取节点宽度
//    console.log(node);
//    //const width = node['width'];
//
//    // 使用节点宽度渲染节点
//    return (
//      <div className="custom-node" style={{ width: `${10}px` }}>
//        {super.render()}
//      </div>
//    );
//  }
//}
//class CustomNodeFactory extends DefaultNodeFactory {
//  generateReactWidget(diagramEngine) {
//    // 自定义节点渲染方法，可以设置宽度
//    console.log('generateReactWidget 1:', diagramEngine);
//    return <CustomNodeWidget node={diagramEngine} />;
//  }
//}

class Node {
  // id
  id: number = 0;

  // 名称
  name?: string;

  // sql 语句
  selectStatement?: string;

  // 端口 ID 到 Node 之间的映射关系
  items: NodeItem[] = [];

  // Node Model react-diagrams node 节点对象
  nodeModel: DefaultNodeModel;

  // 宽度
  width: number;

  // 高度
  height: number;

  // 孩子节点集合
  children: Node[];

  constructor(subQuery: API.WideTableSubquery) {
    this.id = subQuery.id!;
    this.name = subQuery.name;
    this.selectStatement = subQuery.selectStatement;

    this.nodeModel = new DefaultNodeModel({
      name: this.name,
      color: defatulClobalSettings.nodeColor,
      extras: subQuery.id!,
    });

    let maxLen: number = 1;
    for (let col of subQuery.columns) {
      const leftLabel = SqColBeautifier.getBeautifyName(col);
      const rightLabel = SqColBeautifier.getBeautifyType(col);
      this.items.push({
        id: col.id,
        leftLabel: leftLabel,
        rightLabel: rightLabel,
        parentIds: col.parentIds,
        inPort: this.nodeModel.addPort(
          new DefaultPortModel({
            name: col.id + '-in',
            in: true,
            label: leftLabel,
            extras: col.id,
          }),
        ),
        outPort: this.nodeModel.addPort(
          new DefaultPortModel({
            name: col.id + '-out',
            in: false,
            label: rightLabel,
            extras: col.id,
          }),
        ),
        isSelected: false,
      });

      const len = leftLabel.length + rightLabel.length;
      if (len > maxLen) {
        maxLen = len;
      }
    }

    this.width = maxLen;

    this.height = subQuery?.columns || subQuery.columns.length > 0 ? subQuery?.columns.length : 1;

    this.children = [];

    // 限制最大宽度,解决函数类型的字段展示问题
    //if (this.width > defatulClobalSettings.nodeMaxWidth) {
    //  this.nodeModel.width = defatulClobalSettings.nodeMaxWidth;
    //}
    //this.nodeModel['width'] = 10;

    //for (let item of this.items) {
    //  item.inPort!.width = 10;
    //  item.outPort!.width = 10;
    //}
  }

  /**
   * 添加子节点.
   *
   * @param child 子节点
   */
  addChild(child: Node) {
    this.children?.push(child);
  }
}

class DiagramNodeTree {
  // root 节点
  root: Node;

  // 绘图引擎
  diagramEngine: DiagramEngine;

  // portId->Node
  itemIdToNodeMapping: Map<number, Node> = new Map();

  // itemId->NodeItem
  itemIdToItemMapping: Map<number, NodeItem> = new Map();

  // nodeId->Node
  nodeIdtoNodeMapping: Map<number, Node> = new Map();

  // fromItemId->toItemId->NodeLink
  itemIdToLinkMaping: Map<number, Map<number, NodeLink>> = new Map();

  // 树宽度
  treeWidth: number = 0;

  // 树每层节点的宽度
  levelNodeWidth: Map<number, number> = new Map();

  // 树每层节点的高度
  levelNodeHeight: Map<number, number> = new Map();

  // 树深度
  treeDepth: number = 0;

  constructor(root: Node) {
    this.root = root;

    /* 1. 计算 startX
     * 2. 计算 nodeStartY
     * 3. 设置列和关系映射,绘制连接线的时候需要知道 port 和node之前的关系
     */
    let childQueue: Node[] = [this.root];
    let level = 0;
    for (; childQueue.length != 0; ) {
      const nextChildQueue: Node[] = [];

      let curlLevelMaxPortWidth = 0;
      let curlLevelMaxPortHeight = 0;
      for (; childQueue.length != 0; ) {
        const child = childQueue.shift();

        // 计算每层的 node 节点的最大宽度
        if (curlLevelMaxPortWidth < child?.width!) {
          curlLevelMaxPortWidth = child?.width!;
        }

        // 计算每层的 node 节点的最大高度
        if (curlLevelMaxPortHeight < child!.height) {
          curlLevelMaxPortHeight = child!.height;
        }

        // 处理 port 与 node 的映射关系
        this.addItemIdToNodeMapping(child!);
        this.addNodeIdToNodeMapping(child!);
        this.addItemIdToItemMapping(child!);

        nextChildQueue.push(...child?.children!);
      }

      this.levelNodeWidth.set(level, curlLevelMaxPortWidth);
      this.levelNodeHeight.set(level, curlLevelMaxPortHeight);

      // 计算绘图开始的 x 坐标
      this.treeWidth += curlLevelMaxPortWidth;

      childQueue = nextChildQueue;

      level++;
    }

    this.treeDepth = level - 1;

    this.diagramEngine = this.draw();
  }

  /**
   * 获取绘图引擎
   * @returns DiagramEngine
   */
  public getEngine(): DiagramEngine {
    return this.diagramEngine;
  }

  private draw(): DiagramEngine {
    const diagramModel = new DiagramModel();
    const baseModels: BaseModel[] = [];

    // 开始 x 坐标
    const startX =
      this.treeWidth * defatulClobalSettings.spacingUnit +
      defatulClobalSettings.canvasLeftMargin +
      this.treeDepth * defatulClobalSettings.xMargin;

    // 开始 y 坐标
    const startY = defatulClobalSettings.canvasTopMargin;

    // x 方向 magin
    const xMargin = defatulClobalSettings.xMargin;

    // y 方向 magin
    const yMargin = defatulClobalSettings.yMargin;

    let curX: number = startX;
    let curY: number = startY;

    let childQueue: Node[] = [this.root];

    let level = 0;
    for (; childQueue.length != 0; ) {
      // 当前节点 x 坐标位置,取决于树当前层节点中最宽的节点宽度，当前节点x坐标="当前坐标"-"本层最宽节点的宽度"
      curX = curX - this.levelNodeWidth.get(level)! * defatulClobalSettings.spacingUnit;
      console.log(
        ' level: ' +
          level +
          ' curx: ' +
          curX +
          ' levelWidth: ' +
          this.levelNodeWidth.get(level) +
          ' treeWidth: ' +
          this.treeWidth +
          ' startX: ' +
          startX +
          ' treeDepth: ' +
          this.treeDepth,
      );
      const nextChildQueue: Node[] = [];
      for (; childQueue.length != 0; ) {
        const child = childQueue.shift();

        child?.nodeModel.setPosition(curX, curY);

        baseModels.push(child!.nodeModel);

        // 创建连接线
        for (let item of child?.items!) {
          const inItem = item!;
          for (let pId of item.parentIds!) {
            const outItem = this.itemIdToItemMapping.get(pId)!;

            let link = new HorveredLinkModel(this);
            link.setSourcePort(outItem.outPort!);
            link.setTargetPort(inItem.inPort!);
            link.setSelected(false);

            baseModels.push(link);

            this.addItemIdToLinkMaping(outItem.id!, inItem!.id!, link);
          }
        }

        // 同一层节点不需要更改 x 坐标，只需要更改 Y 坐标
        curY = curY + yMargin + child!.height * defatulClobalSettings.spacingUnit;
        nextChildQueue.push(...child?.children!);
      }

      // 进入下一层节点遍历，当前 x 坐标需要减除掉 magin
      curX = curX - xMargin;
      // 每一层的节点 y 的起始坐标都是保持相同的
      curY = startY;

      childQueue = nextChildQueue;

      level++;
    }

    // 添加 所有的model
    diagramModel.addAll(...baseModels);

    // 创建绘图引擎
    const engine = createEngine({
      registerDefaultDeleteItemsAction: true,
      registerDefaultPanAndZoomCanvasAction: true,
      registerDefaultZoomCanvasAction: true,
    });
    engine.getLinkFactories().registerFactory(new HorveredLinkFactory());
    engine.setModel(diagramModel);
    engine.getActionEventBus().registerAction(new ClickAction(this));
    //engine.getNodeFactories().registerFactory(new CustomNodeFactory());

    return engine;
  }

  private addItemIdToNodeMapping(node: Node): void {
    for (let item of node.items) {
      this.itemIdToNodeMapping.set(item.id!, node);
    }
  }

  private addNodeIdToNodeMapping(node: Node): void {
    this.nodeIdtoNodeMapping.set(node.id, node);
  }

  private addItemIdToItemMapping(node: Node): void {
    for (let item of node.items) {
      this.itemIdToItemMapping.set(item.id!, item);
    }
  }

  private addItemIdToLinkMaping(
    outItemId: number,
    inItemId: number,
    link: HorveredLinkModel,
  ): void {
    if (!this.itemIdToLinkMaping.get(outItemId)) {
      const map: Map<number, NodeLink> = new Map();
      this.itemIdToLinkMaping.set(outItemId, map);
    }

    const inMap: Map<number, NodeLink> = this.itemIdToLinkMaping.get(outItemId)!;
    inMap.set(inItemId, new NodeLink(outItemId, inItemId, link));

    this.itemIdToLinkMaping.set(outItemId, inMap);
  }

  nodeOnClick(nodeId: number): void {
    const node: Node = this.nodeIdtoNodeMapping.get(nodeId)!;

    this.unSelectAllLinks();

    this.updateNodeItemRelatedLinksState(node.items, true);
  }

  nodeItemOnClick(itemId: number): void {
    this.unSelectAllLinks();

    const item = this.itemIdToItemMapping.get(itemId)!;
    this.updateItemRelatedLinksState(item.id!, true, true);
  }

  nodeItemOnSelection(itemId: number, isSelected: boolean): void {
    const item = this.itemIdToItemMapping.get(itemId)!;
    this.updateItemRelatedLinksState(item.id!, isSelected, false);
  }

  unSelectAllLinks(): void {
    for (let item of this.itemIdToItemMapping) {
      item[1].isSelected = false;
    }

    for (let entity1 of this.itemIdToLinkMaping.entries()) {
      for (let entity2 of entity1[1].entries()) {
        const nodeLink = entity2[1];
        nodeLink.link.setSelected(false);
      }
    }
  }

  private updateNodeItemRelatedLinksState(items: NodeItem[], state: boolean): void {
    for (let item of items) {
      this.updateItemRelatedLinksState(item.id!, state, true);
    }
  }

  private updateItemRelatedLinksState(
    fromItemId: number,
    state: boolean,
    updateState: boolean,
  ): void {
    const fromItem = this.itemIdToItemMapping.get(fromItemId);
    if (updateState) {
      fromItem!.isSelected = state;
    }

    const cid: number = fromItem!.id!;
    const parentIds: number[] = fromItem?.parentIds!;
    for (let pid of parentIds) {
      const parentItem = this.itemIdToItemMapping.get(fromItemId);
      if (updateState) {
        parentItem!.isSelected = state;
      }

      const link = this.itemIdToLinkMaping.get(pid)?.get(cid);
      link?.link.setSelected(state || parentItem?.isSelected);
      this.updateItemRelatedLinksState(pid, state, updateState);
    }
  }
}

export type SqColLineageDiagramProps = {
  subQuery?: API.WideTableSubquery;
};

const SqColLineageDiagram: React.FC<SqColLineageDiagramProps> = (
  props: SqColLineageDiagramProps,
) => {
  // 使用 memo 防止在没有必要刷新页面的时候反复重新初始化，影响应能,也可以在 useEffect 中判断处理
  //const SqColLineageDiagram = memo((props: SqColLineageDiagramProps) => {
  const { subQuery } = props;

  const [initialized, setInitialized] = useState<boolean>(false);

  // bug 解决： 解决绘图引擎中的数据结构已经改变，但是画布没有刷新，导致显示虽然正常，但是不可以拖拽的 bug
  const [loadV, setLoadV] = useState<number>(-99);

  const [diagramNodeTree, setDiagramNodeTree] = useState<DiagramNodeTree>();

  /**
   * 将后台的子查询结构转换称多叉树结构.
   *
   * 1. 只关注 real 节点
   *
   * 2. real 节点处理子节点的时候，父节点为real 节点，继续递归
   *
   * 3. 非real 节点处理子节点的时候，父节点为栈中栈顶 real 节点，继续递归
   *
   * 4. 递归边界为子节点的集合为空(leftSubquery, rightSubquery,subquery 均为空，则到达边界条件)
   * */
  const transformSubQuery = (children: API.WideTableSubquery[], parent?: Node): void => {
    for (let child of children) {
      if (child.real) {
        const childNode = new Node(child);

        parent?.addChild(childNode);

        transformSubQuery(getchildrenQueue(child), childNode);
      } else {
        transformSubQuery(getchildrenQueue(child), parent);
      }
    }
  };

  const getchildrenQueue = (subQuery?: API.WideTableSubquery): API.WideTableSubquery[] => {
    const childQueue: API.WideTableSubquery[] = [];

    if (subQuery?.leftSubquery) {
      childQueue.push(subQuery.leftSubquery);
    }

    if (subQuery?.rightSubquery) {
      childQueue.push(subQuery.rightSubquery);
    }

    if (subQuery?.subquery) {
      childQueue.push(subQuery.subquery);
    }

    return childQueue;
  };

  /**
   * 初始化
   */
  useEffect(() => {
    initDiagramNodeTree(subQuery);
  }, [subQuery]);

  const initDiagramNodeTree = async (subQuery?: API.WideTableSubquery) => {
    // 重新设置初始化状态为 false, 刷新 version
    setLoadV(loadV + 1);
    setInitialized(false);

    if (!subQuery) {
      return;
    }

    const root: Node = new Node(subQuery!);
    transformSubQuery(getchildrenQueue(subQuery), root);

    const nodeTree = new DiagramNodeTree(root);

    setInitialized(true);
    setDiagramNodeTree(nodeTree);
  };

  return (
    <div style={{ height: '100%' }}>
      {initialized ? (
        <>
          <div style={{ height: '100%', width: '100%' }}>
            <CanvasWidget
              key={'canvas-widget-key-' + loadV}
              className="diagram-container"
              engine={diagramNodeTree!.getEngine()}
            />
          </div>
        </>
      ) : (
        <div style={{ height: '100%', width: '100%' }}>
          <PageLoading />
        </div>
      )}
    </div>
  );
};

export default SqColLineageDiagram;
