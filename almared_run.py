"""pipeline for ALMARED analysis

Contact: Jianhang Chen; cjhastro@gmail.com
Last Update: 2022-01-06
"""

import re
import os
import re
import glob
import numpy as np

########################################
## define the functions for simple tasks
def split_sources(msfile, match='HATLAS_RED_\d+', datacolumn='corrected', 
        outdir='./', spw='5,7,17,19', overwrite=False, **kwargs):
    """this function is used to extract every single target in 
    the snapshoot observation
    """
    if not os.path.isdir(msfile):
        raise ValueError("Cannot find {}".format(msfile))
    if not os.path.isdir(outdir):
        os.system('mkdir {}'.format(outdir))

    field_match = re.compile(match)
    tb.open(os.path.join(msfile, 'FIELD'))
    field_names = tb.getcol('NAME')
    for name in field_names:
        if field_match.match(name):
            ms_field = os.path.join(outdir, name + '.ms')
            if overwrite:
                if os.path.isdir(ms_field):
                    rmtables(ms_field)
            else:
                if os.path.isdir(ms_field):
                    ms_field = ms_field + '.duplicate'
            print('Splitting {}...'.format(name))
            split(vis=msfile, outputvis=ms_field, datacolumn=datacolumn,
                    field=name, spw=spw, **kwargs)

def make_cont_image(vis, basename=None, outdir='./',
        datacolumn='data', imsize=200, cellsize='0.2arcsec', 
        outframe='LSRK', specmode='mfs', 
        threshold='0.3mJy', niter=2000, interactive=False,        
        weighting='natural', usemask='auto-multithresh', 
        clean=True, only_fits=True, debug=False, pbcor=True, overwrite=False,
        **kwargs):
    """the quick function to generate the continuum image
    """
    if basename is None:
        basename = os.path.basename(vis)[:-3]
    if not os.path.isdir(outdir):
        os.system('mkdir {}'.format(outdir))

    imagename = os.path.join(outdir, basename + '.mfs')

    if debug:
        # print out all the parameters
        print("imagename: {}".format(imagename))
        print("imsize: {}".format(imsize))
        print("cellsize: {}".format(cellsize))
        print("threshold: {}".format(threshold))
        print("weighting: {}".format(weighting))
        print("usemask: {}".format(usemask))

    new_clean = True
    if clean:
        if os.path.isfile(imagename+'.image.fits') or os.path.isdir(imagename+'.image'):
            if not overwrite:
                print("{} image already exists. Delete them to overwrite.".format('imagename'))
                new_clean = False
    if new_clean:
        rmtables('{}.*'.format(imagename))
        os.system('rm -rf {}.fits'.format(imagename))
        tclean(vis=vis,
                imagename=imagename,
                imsize=imsize, 
                cell=cellsize, 
                datacolumn=datacolumn, 
                specmode=specmode, 
                outframe=outframe, 
                weighting=weighting,
                niter=niter, 
                threshold=threshold,
                interactive=interactive, 
                usemask=usemask,
                **kwargs)
        if pbcor:
            impbcor(imagename=imagename+'.image',
                    pbimage=imagename+'.pb',
                    outfile=imagename+'.pbcor.image')
        if only_fits:
            exportfits(imagename=imagename+'.image', fitsimage=imagename+'.image.fits')
            exportfits(imagename=imagename+'.pbcor.image', fitsimage=imagename+'.pbcor.image.fits')
            exportfits(imagename=imagename+'.psf', fitsimage=imagename+'.psf.fits')
            exportfits(imagename=imagename+'.pb', fitsimage=imagename+'.pb.fits')
            if debug:
                exportfits(imagename=imagename+'.residual', fitsimage=imagename+'.residual.fits')
            rmtables(tablenames=imagename+'.*')

    else:
        return 0

def make_cube(vis, basename=None, outdir='./',
        datacolumn='data', imsize=200, cellsize='0.2arcsec',
        outframe='LSRK', specmode='cube',
        threshold='5mJy', niter=2000, interactive=False,
        weighting='natural', usemask='auto-multithresh',
        navg=2,
        clean=True, only_fits=True, debug=False, pbcor=True, overwrite=False,
        **kwargs):
    """the quick function to generate the datacube
    """
    if basename is None:
        basename = os.path.basename(vis)[:-3]
    if not os.path.isdir(outdir):
        os.system('mkdir {}'.format(outdir))

    imagename = os.path.join(outdir, basename + '.cube')
    spw_list = read_spw(vis)
    restfreq = "{:.2f}GHz".format(np.mean(spw_list))

    if debug:
        # print out all the parameters
        print("imagename: {}".format(imagename))
        print("imsize: {}".format(imsize))
        print("cellsize: {}".format(cellsize))
        print("threshold: {}".format(threshold))
        print("weighting: {}".format(weighting))
        print("usemask: {}".format(usemask))
        print("spw list: {}".format(spw_list))
    
    for i in range(len(spw_list)):
        new_clean = True
        spw = str(i)
        freq_low, freq_up, freq_interv, freq_nchan = spw_list[i]
        start = "{:.2f}GHz".format(freq_low + freq_interv*2.5)
        width = "{:.2f}MHz".format(freq_interv * 1000.0)
        nchan = int(freq_nchan - 5)
        veltype = 'optical'

        subcube_name = imagename + '.spw' + spw

        if debug:
            # print out all the parameters
            print(">> spw{}:".format(spw))
            print("subcube_name: {}".format(subcube_name))
            print("start: {}".format(start))
            print("width: {}".format(width))
            print("nchan: {}".format(nchan))

        if new_clean:
            rmtables('{}.*'.format(subcube_name))
            os.system('rm -rf {}.fits'.format(subcube_name))
            tclean(vis=vis, imagename=subcube_name,
                    spw=spw, imsize=imsize, cell=cellsize, 
                    start=start, nchan=nchan, #width=width, 
                    datacolumn=datacolumn, specmode=specmode, outframe=outframe, 
                    weighting=weighting, usemask=usemask,
                    niter=niter, threshold=threshold, interactive=interactive, 
                    **kwargs)
            if pbcor:
                impbcor(imagename=subcube_name+'.image',
                        pbimage=subcube_name+'.pb',
                        outfile=subcube_name+'.pbcor.image')
        if only_fits:
            exportfits(imagename=subcube_name+'.image', fitsimage=subcube_name+'.image.fits')
            exportfits(imagename=subcube_name+'.pbcor.image', fitsimage=subcube_name+'.pbcor.image.fits')
            # exportfits(imagename=subcube_name+'.psf', fitsimage=subcube_name+'.psf.fits')
            # exportfits(imagename=subcube_name+'.pb', fitsimage=subcube_name+'.pb.fits')
    if only_fits:
        rmtables(tablenames=imagename+'.*')


#############################################
## pipeline functions
def run_get_calibrated_data(casapipeline, basedir):
    """Reproduce all the calibrated data
    """
    for science_goal in os.listdir(basedir):
        for group in os.listdir(os.path.join(basedir, science_goal)):
            for member in os.listdir(os.path.join(basedir, science_goal, group)):
                calibrated_dir = os.path.join(basedir, science_goal, group, member, 'calibrated')
                script_dir = os.path.join(basedir, science_goal, group, member, 'script')
                if os.path.isdir(calibrated_dir):
                    print("{}/{}/{} is Done!".format(science_goal, group, member))
                else:
                    print("{}/{}/{} is NOT Done!".format(science_goal, group, member))
                    os.chdir(script_dir)
                    #print(os.path.isfile('{}.scriptForPI.py'.format(member)))
                    os.system('{} --pipeline -c *.scriptForPI.py'.format(casa_pipeline))

def run_split_sources(basedir, outdir=None, **kwargs):
    """split out all the targets calibrated data within the project folder
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

def run_make_all_cubes(visdir=None, outdir=None):
    """generate images for all the visibilies in one directory
    """
    if not os.path.isdir(outdir):
        os.system('mkdir {}'.format(outdir))
    for vis in os.listdir(visdir):
        make_cube(os.path.join(visdir,vis), outdir=outdir)

def run_make_jackknif_vis(visdir=None, outdir=None, image_outdir=None):
    """generating jackknif images for all the visibilies
    """
    if not os.path.isdir(outdir):
        os.system('mkdir {}'.format(outdir))
    for vis in os.listdir(visdir):
        vis_copy = vis_jackknif(os.path.join(visdir,vis), copy=True, outdir=outdir)
        make_cont_image(vis_copy, outdir=image_outdir, basename=os.path.basename(vis_copy)[:-8]+'.jackknif')

