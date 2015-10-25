package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML;

public class HsSlowTasks extends HsView{

	@Override
	protected Class<? extends SubView> content() {
		return HsSlowTasksBlock.class;
	}

  @Override
  protected void commonPreHead(HTML<_> html) {
    set(ACCORDION_ID, "nav");
    //override the nav config from commonPReHead
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:2}");
  }
}
