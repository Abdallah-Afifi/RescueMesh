"""Figure 2 of the revised survey: request flow with the coordinator ordering commits."""
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Polygon

INK, MUTED, LINE, FILL, ACC, ACCF = '#1f2933', '#52606d', '#7b8794', '#ffffff', '#1f6feb', '#e6f0ff'
fig, ax = plt.subplots(figsize=(8.6, 9.4), dpi=200)
ax.set_xlim(0, 86); ax.set_ylim(0, 94); ax.axis('off')

def box(x, y, w, h, title, sub=None, accent=False):
    ax.add_patch(FancyBboxPatch((x - w/2, y - h/2), w, h, boxstyle='round,pad=0.25,rounding_size=1.2',
                 fc=ACCF if accent else FILL, ec=ACC if accent else LINE, lw=2.0 if accent else 1.3))
    ax.text(x, y + (1.25 if sub else 0), title, ha='center', va='center', fontsize=9.5, fontweight='bold', color=INK)
    if sub: ax.text(x, y - 1.6, sub, ha='center', va='center', fontsize=8, color=MUTED)

def diamond(x, y, w, h, label):
    ax.add_patch(Polygon([(x, y + h/2), (x + w/2, y), (x, y - h/2), (x - w/2, y)], closed=True, fc=FILL, ec=LINE, lw=1.3))
    ax.text(x, y, label, ha='center', va='center', fontsize=8.8, color=INK)

def arrow(p, q, label=None, lab_xy=None, dashed=False, rad=0.0):
    ax.annotate('', xy=q, xytext=p, arrowprops=dict(arrowstyle='-|>', color=LINE, lw=1.3,
                linestyle='--' if dashed else '-', connectionstyle=f'arc3,rad={rad}', shrinkA=0, shrinkB=0))
    if label: ax.text(*lab_xy, label, ha='center', va='center', fontsize=8, color=MUTED)

ax.text(43, 91.5, 'One committed result per request, ordered by the coordinator',
        ha='center', va='center', fontsize=11.5, fontweight='bold', color=INK)

X, W, H = 30, 40, 6.6
ys = dict(req=83, rank=72.5, claim=62, prop=51.5, dup=41, ver=29.5, app=18.5, done=8)
box(X, ys['req'], W, H, 'Client multicasts REQ', 'reqID, payload and measured RTTs, to all 3 nodes')
box(X, ys['rank'], W, H, 'Each ACTIVE node ranks the view', 'load score + claims seen; hash tie-break')
box(X, ys['claim'], W, H, 'Rank 1 sends CLAIM and plans', 'records the version of each resource it reads')
box(X, ys['prop'], W, H, 'Executor sends PROPOSE', 'plan and versions read, to the coordinator')
diamond(X, ys['dup'], 32, 8, 'reqID already in the log?')
diamond(X, ys['ver'], 32, 8, 'Resources unchanged?')
box(X, ys['app'], W, H, 'Coordinator appends the entry', 'next sequence number; sent to both peers')
box(X, ys['done'], W, H, 'Committed, then reply', 'after 1 peer acknowledges (majority of 3)', accent=True)

for a, b in [('req', 'rank'), ('rank', 'claim'), ('claim', 'prop')]:
    arrow((X, ys[a] - H/2 - 0.3), (X, ys[b] + H/2 + 0.3))
arrow((X, ys['prop'] - H/2 - 0.3), (X, ys['dup'] + 4))
arrow((X, ys['dup'] - 4), (X, ys['ver'] + 4), 'no', (X + 2.2, ys['dup'] - 5.6))
arrow((X, ys['ver'] - 4), (X, ys['app'] + H/2 + 0.3), 'yes', (X + 2.4, ys['ver'] - 5.6))
arrow((X, ys['app'] - H/2 - 0.3), (X, ys['done'] + H/2 + 0.3))

RX, RW = 70, 27
box(RX, ys['req'], RW, H, 'Client retry, same reqID', 'committed: stored result returned')
arrow((X + W/2 + 0.3, ys['req']), (RX - RW/2 - 0.3, ys['req']), 'timeout', (53.2, ys['req'] + 2.0), dashed=True)
box(RX, ys['dup'], RW, H, 'Return the stored result', 'nothing is executed twice')
arrow((X + 16, ys['dup']), (RX - RW/2 - 0.3, ys['dup']), 'yes', (51.5, ys['dup'] + 1.5))
box(RX, ys['ver'], RW, H, 'REJECT: plan again', 'from a fresh snapshot')
arrow((X + 16, ys['ver']), (RX - RW/2 - 0.3, ys['ver']), 'no', (51.5, ys['ver'] + 1.5))
# re-plan loop back to the claim step
ax.plot([RX + RW/2 + 0.3, 85.2, 85.2], [ys['ver'], ys['ver'], ys['claim'] - 1.6], color=LINE, lw=1.3)
arrow((85.2, ys['claim'] - 1.6), (X + W/2 + 0.3, ys['claim'] - 1.6))
ry = ys['rank'] - 1.0
ax.add_patch(FancyBboxPatch((RX - RW/2, ry - 4.6), RW, 9.2, boxstyle='round,pad=0.25,rounding_size=1.2', fc=FILL, ec=LINE, lw=1.3))
ax.text(RX, ry + 2.6, 'Recovery', ha='center', va='center', fontsize=9.5, fontweight='bold', color=INK)
ax.text(RX, ry - 1.2, 'no CLAIM within T_c: next rank claims\nclaimer leaves the view: ranked again',
        ha='center', va='center', fontsize=7.6, color=MUTED, linespacing=1.5)
arrow((RX - RW/2 - 0.3, ys['rank']), (X + W/2 + 0.3, ys['rank']))
fig.savefig(sys.argv[1], bbox_inches='tight', facecolor='white')
