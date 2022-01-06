"""pipeline for ALMARED analysis

Contact: Jianhang Chen; cjhastro@gmail.com
Last Update: 2022-01-06
"""

import re

def run_split_sources(basedir, outdir=None, **kwargs):
    """split out all the targets calibrated data with in the project folder
    """
    if not os.path.isdir(outdir):
        os.system('mkdir {}'.format(outdir))
    vis_match = re.compile('uid___\w*.ms')
    for science_goal in os.listdir(basedir):
        for group in os.listdir(os.path.join(basedir, science_goal)):
            for member in os.listdir(os.path.join(basedir, science_goal, group)):
                calibrated_dir = os.path.join(basedir, science_goal, group, member, 'calibrated')
                for item in os.listdir(calibrated_dir):
                    if vis_match.match(item):
                        calibrated_vis = os.path.join(calibrated_dir, item)
                        print(calibrated_vis)
                        split_sources(calibrated_vis, outdir=outdir, **kwargs)

def run_make_all_cont_images(visdir=None, outdir=None):
    """generate images for all the visibilies in one directory
    """
    if not os.path.isdir(outdir):
        os.system('mkdir {}'.format(outdir))
    for vis in os.listdir(visdir):
        make_cont_image(os.path.join(visdir,vis), outdir=outdir)
        if vis[-9:] == 'duplicate':
            vis_first = vis[:-10]
            vis_combined = vis_first[:-3] + '.combine.ms'
            vis_list = [os.path.join(visdir, vis), os.path.join(visdir, vis_first)]
            concatvis = os.path.join(visdir, vis_combined)
            concat(vis=vis_list, concatvis=concatvis)
            make_cont_image(concatvis, outdir=outdir)
