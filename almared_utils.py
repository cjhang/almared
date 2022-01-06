import os
import re
import glob
import numpy as np

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

def read_spw(vis):
    """read the spectral windows
    """
    tb = tbtool()
    if isinstance(vis, str):
        vis = [vis, ]
    
    if not isinstance(vis, list):
        raise ValueError("read_spw: Unsupported measurements files!")
    
    spw_specrange = {}

    for v in vis:
        tb.open(v + '/SPECTRAL_WINDOW')
        col_names = tb.getvarcol('NAME')
        col_freq = tb.getvarcol('CHAN_FREQ')
        tb.close()

        for key in col_names.keys():
            freq_max = np.max(col_freq[key]) / 1e9
            freq_min = np.min(col_freq[key]) / 1e9
            freq_interv = np.abs(np.mean(np.diff(col_freq[key], axis=0))) / 1e9
            freq_nchan = len(col_freq[key])
            spw_specrange[key] = [freq_min, freq_max, freq_interv, freq_nchan]

    return list(spw_specrange.values())

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

   
